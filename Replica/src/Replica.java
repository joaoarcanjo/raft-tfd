import com.google.gson.Gson;
import events.*;
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
    private static EventLogic eventLogic;
    private static Condition condition;

    public static final BlockingQueue<Result> blockingQueue = new LinkedBlockingQueue<>();

    private static boolean terminate = false;
    private static AtomicInteger waitingResults;
    private static AtomicReference<Timestamp> lastRequestTimestamp;
    /**
     * Identifier of the current replica
     */
    private static int replicaId;
    private static State state;

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
            Result result;
            int observedValue;
            while (!terminate) {
                try {
                    result = blockingQueue.take(); // TODO: Place signal somewhere around here

                    if(state.getCurrentTerm() < Integer.parseInt(result.getResultMessage())) {
                        condition.notify();
                    }
                    observedValue = waitingResults.get();
                    if (observedValue > 0 && !waitingResults.compareAndSet(observedValue, --observedValue)) {
                        throw new IllegalStateException("Some concurrent problem is happening...");
                    }
                    if (observedValue > 0) {
                        System.out.println("ResultMessage from " + result.getId() + ": " + result.getResultMessage());
                        continue;
                    }
                    // If the timestamps are different, then a result from a previous request arrived
                    if (lastRequestTimestamp.get() == null || !lastRequestTimestamp.get().equals(result.getTimestamp()))
                        continue;
                    waitingResults.notify();
                    resetRequestTimestamp();
                    if (result.hasResults()) {
                        System.out.println("ResultList: " + result.getResults().getListList());
                        continue;
                    }
                    System.out.println("ResultMessage from " + result.getId() + ": " + result.getResultMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        resultsThread.start();
        return resultsThread;
    }

    /**
     * Initializes the initial properties of a replica, i.e. its id and the addresses of the other replicas
     *
     * @param id             current replica's id
     * @param configFilePath absolute path to the configuration text file with the replicas' address
     * @throws IOException in case an I/O error occurs while reading the file
     */
    private static void initReplica(int id, String configFilePath) throws IOException {
        replicaId = id;
        condition = new ReentrantLock().newCondition();
        readConfigFile(configFilePath);
        eventLogic = new EventLogic(condition);
        waitingResults = new AtomicInteger(0);
        lastRequestTimestamp = new AtomicReference<>(null);
        serverThread = GRPCServer.initServerThread(replicas.get(replicaId).getFirst().getPort(), eventLogic);
        resultsThread = initResultsThread();
        state = new State();
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
    private static void invoke(int destinyReplicaId, String requestLabel, String requestData, Timestamp timestamp) {
        if (destinyReplicaId == replicaId) {
            eventLogic.getEventHandler(requestLabel)
                    .ifPresentOrElse(
                            eventHandler -> eventHandler.processSelfRequest(requestLabel, requestData),
                            () -> { throw new IllegalArgumentException("Invalid label"); }
                    );
            resetRequestTimestamp();
            waitingResults.set(0);
            return;
        }

        Request request = Request.newBuilder()
                .setId(destinyReplicaId)
                .setLabel(requestLabel)
                .setData(requestData)
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
    private static void quorumInvoke(String requestLabel, String requestData, Timestamp timestamp) {
        for (int id = 0; id < replicas.size(); id++) {
            invoke(id, requestLabel, requestData, timestamp);
        }
    }

    /**
     * Cancels the wait for results from a previous operation request
     *
     * @param scanner scanner to read from the standard input
     */
    private static void cancelOperation(Scanner scanner) {
        System.out.println("Do you want to cancel the previous operation? [y/n]");
        System.out.print("-> ");
        String response = scanner.nextLine();
        if (response.length() > 0 && response.compareTo("y") != 0) return;
        resetRequestTimestamp();
        waitingResults.set(0);
    }

    /**
     * Menu with all the possible operations to test the system
     */
    private static void operations() {
        Scanner scanner = new Scanner(System.in);
        String options = "Choose an operation: \n" +
                " [0] ADD string to all replicas\n" +
                " [1] GET set of strings from a replica\n" +
                " [2] Exit";
        while (!terminate) {
            System.out.println(options);
            System.out.print("-> ");

            switch (scanner.nextLine()) {
                case "0": {
                    // TODO: Se forem 5 réplicas, esperamos por 3 respostas ou 2 (se a replica atual não contar)?
                    if (!waitingResults.compareAndSet(0, Math.round(replicas.size() / 2f))) { // k > n/2 - 1
                        System.out.println(" * Still waiting for the majority of results...");
                        cancelOperation(scanner);
                        break;
                    }
                    System.out.print("Insert the data: \n-> ");
                    String data = scanner.nextLine();

                    Timestamp rpcTimestamp = getInstantTimestamp();
                    lastRequestTimestamp.set(rpcTimestamp);
                    quorumInvoke(AddEvent.LABEL, data, rpcTimestamp);
                    break;
                }
                case "1": {
                    if (!waitingResults.compareAndSet(0, 1)) {
                        System.out.println(" * Still waiting for the result...");
                        cancelOperation(scanner);
                        break;
                    }

                    System.out.print("Replica id: \n-> ");
                    int id = Integer.parseInt(scanner.nextLine());

                    while(id < 0 || id >= replicas.size()) {
                        System.out.println("Please provide an Id that exists");
                        System.out.print("Replica id: \n-> ");
                        id = Integer.parseInt(scanner.nextLine());
                    }

                    Timestamp rpcTimestamp = getInstantTimestamp();
                    lastRequestTimestamp.set(rpcTimestamp);
                    invoke(id, GetEvent.LABEL, "", rpcTimestamp);
                    break;
                }
                case "2": {
                    terminate = true;
                    break;
                }
            }
        }
        System.out.println(" * Replica " + replicaId + " is shutting down...");
        System.exit(1);
    }

    static class RequestVoteArgs {
        public int term;
        public int candidateId;
        public int lastLogIndex;
        public int lastLogTerm;

        public RequestVoteArgs(int term, int candidateId, int lastLogIndex, int lastLogTerm) {
            this.term = term;
            this.candidateId = candidateId;
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }
    }

    private static String getRequestVoteArgs() {
        RequestVoteArgs requestVoteArgs = new RequestVoteArgs(
                state.getCurrentTerm(),
                replicaId,
                state.getLastLogIndex(),
                state.getLastLogTerm()
        );
        return new Gson().toJson(requestVoteArgs);
    }

    private static void heartbeat() throws InterruptedException {
        //quando recebe algo de um follower com um term superior
        do {
            quorumInvoke(AppendEntriesEvent.LABEL, "", getInstantTimestamp());
        } while(condition.await(Utils.randomizedTimer(5, 15), TimeUnit.SECONDS));
    }

    private static void leaderElection() throws InterruptedException {
        while (true) {
            // timer: timeout / heartbeat / requestVote / appendEntry
            boolean heartbeat = false;
            if (state.getCurrentState() != State.ReplicaState.CANDIDATE) {
                heartbeat = condition.await(Utils.randomizedTimer(5, 15), TimeUnit.SECONDS);
            }
            if(!heartbeat) {
                //vai para candidate
                state.setCurrentState(State.ReplicaState.CANDIDATE);
                state.incCurrentTerm();

                Timestamp rpcTimestamp = getInstantTimestamp();
                quorumInvoke(RequestVoteEvent.LABEL, getRequestVoteArgs(), rpcTimestamp);

                boolean notified = condition.await(Utils.randomizedTimer(5, 15), TimeUnit.SECONDS);
                if (notified && waitingResults.get() == 0) {
                    state.setCurrentState(State.ReplicaState.LEADER);
                } else {
                    heartbeat();
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
    }

    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.out.println("Usage: java -jar Replica.jar <id(>= 0)> <configFile(absolute path)>");
                System.exit(-1);
            }
            initReplica(Integer.parseInt(args[0]), args[1]);
            System.out.println(" * REPLICA ID: " + replicaId + " *");
            // operations();
            leaderElection();
        } catch (IOException | InterruptedException e) {
            System.out.println("* ERROR * " + e);
        }
    }
}
