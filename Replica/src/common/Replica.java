package common;

import com.google.protobuf.ByteString;
import events.*;
import events.models.AppendEntriesRPC;
import events.models.LogElement;
import events.models.RequestVoteRPC;
import events.models.State;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.google.protobuf.Timestamp;
import replica.Request;
import replica.Result;
import replica.ServerGrpc;
import streamobservers.ClientStreamObserver;
import utils.Pair;
import utils.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Replica {
    /**
     * Each position of the list correspond to an id of a replica, which contains its socket address
     */
    private static final List<Pair<ReplicaAddress, ServerGrpc.ServerStub>> replicas = new ArrayList<>();
    private static Thread serverThread;
    private static Thread resultsThread;
    private static Thread requestsThread;
    private static EventLogic eventLogic;
    private static Condition condition;
    private static ReentrantLock monitor;
    public static final BlockingQueue<Result> blockingQueue = new LinkedBlockingQueue<>();
    public static final BlockingQueue<Request> blockingQueueClient = new LinkedBlockingQueue<>();
    private static boolean terminate = false;
    private static AtomicInteger waitingResults;
    private static AtomicReference<Timestamp> lastRequestTimestamp; //timestamp do vote.
    //Quorum invoke flag.
    private static AtomicBoolean entriesSent;
    /**
     * Identifier of the current replica
     */
    private static int replicaId;
    private static State state;

    /**
     * Leader election intervals
     */
    private static final Pair<Integer, Integer> SEND_HEARTBEAT_INTERVAL = new Pair<>(5, 7);
    private static final Pair<Integer, Integer> WAIT_VOTES_INTERVAL = new Pair<>(13, 15);
    private static final Pair<Integer, Integer> WAIT_HEARTBEAT_INTERVAL = new Pair<>(13, 15);

    /**
     * Initializes the client communication channel
     *
     * @param replicaAddress address of the replica to establish the connection
     * @return the stub created
     */
    private static ServerGrpc.ServerStub initStub(ReplicaAddress replicaAddress) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(replicaAddress.getIp(), replicaAddress.getPort()).usePlaintext().build();
        return ServerGrpc.newStub(channel);
    }

    /**
     * Reads each line from the file provided and stores for each line the ReplicaAddress and the stub channel in a pair
     * of the list replicas
     *
     * @param configFilePath absolute path to the configuration text file with the replicas' addresses
     * @throws IOException in case an I/O error occurs
     */
    private static void readConfigFile(String configFilePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(configFilePath))) {
            String address;
            ReplicaAddress replicaAddress;
            while ((address = br.readLine()) != null) {
                replicaAddress = new ReplicaAddress(address);
                replicas.add(new Pair<>(replicaAddress, replicaId != replicas.size() ? initStub(replicaAddress) : null // To not initiate a client stub to itself
                ));
            }
        }
    }
    /**
     * Initializes the thread responsible for waiting for the results to arrive and then processes the results obtained
     *
     * @return the results thread instance
     */
    private static Thread initResultsThread() {
        Thread resultsThread = new Thread(() -> {
            monitor.lock();
            try {
                Result result;
                int observedValue;
                while (!terminate) {
                    try {
                        monitor.unlock();
                        result = blockingQueue.take(); //resposta do voto. //resposta do appendEntries
                        monitor.lock();
                        if (state.getCurrentState() == State.ReplicaState.CANDIDATE) {
                            RequestVoteRPC.ResultVote received = RequestVoteRPC.resultVoteFromJson(result.getResultMessage());
                            if (state.getCurrentTerm() < received.term) {
                                System.out.println("$ Updated term to: " + received.term);
                                state.setCurrentTerm(received.term);
                                condition.signal();
                            }

                            if (!received.vote) {
                                continue;
                            } else {
                                System.out.println("# Vote received from replica " + result.getId() + " #");
                            }
                        }

                        if(state.getCurrentState() == State.ReplicaState.LEADER) {
                            //waitingResults.set(0);
                            appendEntryResponse(result);
                            continue;
                        }

                        observedValue = waitingResults.get();

                        if (observedValue > 0 && !waitingResults.compareAndSet(observedValue, --observedValue)) {
                            throw new IllegalStateException("Some concurrent problem is happening...");
                        }
                        if (observedValue > 0) {
                            //System.out.println("+ ResultMessage from " + result.getId() + ": " + result.getResultMessage());
                            continue;
                        }
                        if(state.getCurrentState() == State.ReplicaState.CANDIDATE && observedValue == 0) {
                            condition.signal();
                        }

                        // If the timestamps are different, then a result from a previous request arrived
                        if (lastRequestTimestamp.get() == null || !lastRequestTimestamp.get().equals(result.getTimestamp()))
                            continue;

                        resetRequestTimestamp();
                        if (!result.getResults().isEmpty()) {
                            System.out.println("ResultList: " + result.getResults());
                            continue;
                        }
                        System.out.println("ResultMessage from " + result.getId() + ": " + result.getResultMessage());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            } finally {
                monitor.unlock();
            }
        });
        resultsThread.start();
        return resultsThread;
    }

    private static Thread readClientRequests() {
        Thread resultsThread = new Thread(() -> {
            try {
                while (!terminate) {
                    Request request = blockingQueueClient.take();

                    LogElement.LogElementArgs newLogEntry = new LogElement.LogElementArgs(
                            request.getData().toByteArray(),
                            request.getLabel(),
                            state.getCurrentTerm()
                    );

                    switch (request.getLabel()) {
                        case (IncreaseByEvent.LABEL): {
                            int arg = ByteBuffer.wrap(newLogEntry.getCommandArgs()).getInt();
                            System.out.println("arg ----> " + arg);
                            if(arg < 1 || arg > 5) {
                                throw new Exception();
                            }
                            state.addToLog(newLogEntry);
                            state.incNextIndex(replicaId);
                            quorumInvoke(AppendEntriesEvent.LABEL, newLogEntry, request.getTimestamp());
                            break;
                        }
                        default:
                            System.out.println("-> Unrecognizable label.");
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        });
        resultsThread.start();
        return resultsThread;
    }


    private static void appendEntryResponse(Result result) {

        AppendEntriesRPC.ResultAppendEntry received =
                AppendEntriesRPC.resultAppendEntryFromJson(result.getResultMessage());

        if (received != null && result.getLabel().equals("entriesResponse")) {
            System.out.println("RESULT ARRIVED.");
            System.out.println("_________________________________");
            System.out.println("LOG: Reply received by : "+ result.getId());
            System.out.println("---> NextIndex: "+received.nextIndex);
            System.out.println("_________________________________");
            state.setNextIndex(result.getId(), received.nextIndex);
            state.updateCommitIndexLeader();
        }
    }

    /**
     * Initializes the initial properties of a replica, i.e. its id and the addresses of the other replicas
     *
     * @param id             current replica's id
     * @param configFilePath absolute path to the configuration text file with the replicas' address
     * @throws IOException in case an I/O error occurs while reading the file
     */
    private static void initReplica(int id, String configFilePath, int term) throws IOException {
    //private static void initReplica(int id, String configFilePath) throws IOException {
        replicaId = id;
        monitor = new ReentrantLock();
        condition = monitor.newCondition();
        readConfigFile(configFilePath);
        waitingResults = new AtomicInteger(0);
        lastRequestTimestamp = new AtomicReference<>(null);
        resultsThread = initResultsThread();
        state = new State(term, replicaId);
        //state = new State();
        eventLogic = new EventLogic(monitor, condition, state);
        serverThread = GRPCServer.initServerThread(replicas.get(replicaId).getFirst().getPort(), eventLogic, state);
        requestsThread = readClientRequests();
        entriesSent = new AtomicBoolean();
    }

    /**
     * Obtains the current timestamp from the instant of the system clock and builds the proto Timestamp type
     *
     * @return the proto Timestamp type
     */
    private static Timestamp getInstantTimestamp() {
        var timestamp = java.sql.Timestamp.from(Instant.now());
        return Timestamp.newBuilder().setSeconds(timestamp.getTime()).setNanos(timestamp.getNanos()).build();
    }

    /**
     * Resets the last requested timestamp, changing its value to null
     */
    private static void resetRequestTimestamp() {
        lastRequestTimestamp.set(null);
    }

    /**
     * Invokes a specific operation in another replica
     *
     * @param destinyReplicaId replica to perform the operation
     * @param requestLabel     label associated to the operation to perform
     * @param requestData      data to be sent in the operation
     * @param timestamp        timestamp of the request invocation
     */
    private static void invoke(int destinyReplicaId, String requestLabel, byte[] requestData, Timestamp timestamp) {
        if (destinyReplicaId == replicaId) {
            eventLogic.getEventHandler(requestLabel)
                    .ifPresentOrElse(
                            eventHandler -> eventHandler.processSelfRequest(requestLabel, ByteString.copyFrom(requestData).toStringUtf8()),
                            () -> {
                                throw new IllegalArgumentException("Invalid label");
                            }
                    );
            //resetRequestTimestamp(); //?
            //waitingResults.set(0); //?
            //waitingResults.decrementAndGet();
            return;
        }

        Request request = Request.newBuilder()
                .setId(destinyReplicaId)
                .setLabel(requestLabel)
                .setData(ByteString.copyFrom(requestData))
                .setTimestamp(timestamp)
                .build();
        replicas.get(destinyReplicaId).getSecond().invoke(request, new ClientStreamObserver(blockingQueue));
    }

    /**
     * Invokes an operation in all replicas but the sender
     *
     * @param requestLabel label associated to the operation to perform
     * @param requestData  data to send in the operation
     * @param timestamp    timestamp of the request invocation
     */
    public static void quorumInvoke(String requestLabel, byte[] requestData, Timestamp timestamp) {
        for (int id = 0; id < replicas.size(); id++) {
            invoke(id, requestLabel, requestData, timestamp);
        }
    }

    public static void quorumInvoke(String requestLabel, LogElement.LogElementArgs newLogEntry, Timestamp timestamp) {
        System.out.println("Sending client request to all followers...");
        for (int id = 0; id < replicas.size(); id++) {
            LinkedList<LogElement.LogElementArgs> entries = new LinkedList<>(state.getEntries(id));
            entries.add(newLogEntry);
            LogElement.LogElementArgs prevLog = state.getEntry(state.getNextIndex(id) - 1);
            invoke(
                    id,
                    requestLabel,
                    AppendEntriesRPC.appendEntriesArgsToJson(
                            state,
                            entries,
                            state.getNextIndex(id) - 1,
                            prevLog
                    ).getBytes(),
                    timestamp
            );
        }
        System.out.println("My machine state: " + state.getStateMachine().getCounter());
        entriesSent.set(true);
    }

    private static void heartbeat() throws InterruptedException {
        do {
            if(entriesSent.get()) {
                entriesSent.set(false);
            } else {
                System.out.println("* Heartbeats sent *");
                Timestamp rpcTimestamp = getInstantTimestamp();
                lastRequestTimestamp.set(rpcTimestamp);

                quorumInvoke(
                        AppendEntriesEvent.LABEL,
                        AppendEntriesRPC.appendEntriesArgsToJson(state, new LinkedList<>()).getBytes(),
                        rpcTimestamp
                );
            }
        } while(!condition.await(Utils.randomizedTimer(
                        SEND_HEARTBEAT_INTERVAL.getFirst(),
                        SEND_HEARTBEAT_INTERVAL.getSecond()),
                TimeUnit.SECONDS)
        );
    }

    private static void leaderElection() throws InterruptedException {
        monitor.lock();
        try {
            while (true) {
                boolean notified = false;
                if (state.getCurrentState() == State.ReplicaState.FOLLOWER) {

                    int time = Utils.randomizedTimer(
                            WAIT_HEARTBEAT_INTERVAL.getFirst(),
                            WAIT_HEARTBEAT_INTERVAL.getSecond()
                    );
                    /*System.out.println(
                            "--- Waiting " + time + " seconds for a heartbeat. " +
                            "Term: " + state.getCurrentTerm() + " ---"
                    );*/
                    notified = condition.await(time, TimeUnit.SECONDS);
                    if (!notified) {
                        System.out.println("* Heartbeat timeout *");
                        //System.out.println("-> Switched to candidate\n");
                    }
                }

                if (!notified) {
                    state.incCurrentTerm();
                    state.setCurrentState(State.ReplicaState.CANDIDATE);
                    System.out.println("-> Switched to CANDIDATE. Term: " + state.getCurrentTerm());

                    int votesWaiting = replicas.size() / 2;
                    waitingResults.set(votesWaiting);

                    Timestamp rpcTimestamp = getInstantTimestamp();
                    lastRequestTimestamp.set(rpcTimestamp);
                    quorumInvoke(
                            RequestVoteEvent.LABEL,
                            RequestVoteRPC.requestVoteArgsToJson(state, replicaId).getBytes(),
                            rpcTimestamp);

                    int time = Utils.randomizedTimer(WAIT_VOTES_INTERVAL.getFirst(), WAIT_VOTES_INTERVAL.getSecond());
                    System.out.println("--- Waiting " + time + " seconds for votes ---");
                    notified = condition.await(time, TimeUnit.SECONDS);

                    //Se tiver sido notificado e ter obtido a maioria dos votos, vai ser lider
                    if (notified && waitingResults.get() == 0) {
                        state.InitLeaderState(replicas.size());
                        System.out.println("\n-> Switched to LEADER. Term: " + state.getCurrentTerm());
                        state.setCurrentState(State.ReplicaState.LEADER);
                        state.setCurrentLeader(replicaId);
                        System.out.println("* Start sending heartbeats *");
                        heartbeat();
                        System.out.println("! Leader role lost, switching to follower !");
                        state.setCurrentState(State.ReplicaState.FOLLOWER);
                    }
                    //se foi notificado é porque recebeu um heartbeat, há outro lider
                    else if (notified) {
                        System.out.println("\n-> Switched to FOLLOWER. Term: " + state.getCurrentTerm());
                        state.setCurrentState(State.ReplicaState.FOLLOWER);
                    }
                }

                // if follower: no heartbeat -> changes to candidate and requests votes //OK
                // if follower: receives heartbeat -> resets timer and awaits //OK
                // if follower: receives a requestVote -> ?
                // if candidate: not enough votes and no heartbeat -> send requestVotes again //OK
                // if candidate: enough votes, change to leader and invoke heartbeats //OK
                // if candidate: receives heartbeat and the leader is legit, changes to follower
                // if candidate: receives heartbeat and the leader isn't legit (lower term), continues in candidate state
                // if leader: send heartbeats or log entries
            }
        } finally {
            monitor.unlock();
        }
    }

    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.out.println("Usage: java -jar common.Replica.jar <id(>= 0)> <configFile(absolute path)>");
                System.exit(-1);
            }
            int term = 0;
            if(args.length > 2)
                term = Integer.parseInt(args[2]);
            System.out.println("Start Term: " + term);
            args[0] = auxToDelete();
            initReplica(Integer.parseInt(args[0]), args[1], term);
            //initReplica(Integer.parseInt(args[0]), args[1]);
            System.out.println(" * REPLICA ID: " + replicaId + " *");
            // operations();
            leaderElection();
        } catch (IOException | InterruptedException e) {
            System.out.println("* ERROR * " + e);
        }
    }

    //apenas para poder criar varias instancias sem ter que estar a alterar o argument 0.
    public static String auxToDelete() {
        Scanner scanner = new Scanner(System.in);
        return scanner.nextLine();
    }
}
