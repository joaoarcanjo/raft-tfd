import events.EventLogic;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.google.protobuf.Timestamp;
import replica.Request;
import replica.Result;
import replica.ServerGrpc;
import streamobservers.ClientStreamObserver;
import utils.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Replica {
    /**
     * Each position of the list correspond to an id of a replica, which contains its socket address
     */
    private static final List<Pair<ReplicaAddress, ServerGrpc.ServerStub>> replicas = new ArrayList<>();
    private static Thread serverThread;
    private static Thread resultsThread;
    private static EventLogic eventLogic;

    public static final BlockingQueue<Result> blockingQueue = new LinkedBlockingQueue<>();

    private static AtomicInteger waitingResults;
    private static AtomicReference<Timestamp> lastRequestTimestamp;
    private static int replicaId;

    /**
     * Initializes the client communication channel
     * @param replicaAddress address of the replica to establish the connection
     * @return the stub created
     */
    private static ServerGrpc.ServerStub initStub(ReplicaAddress replicaAddress) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(replicaAddress.getIp(), replicaAddress.getPort())
                .usePlaintext()
                .build();
        return ServerGrpc.newStub(channel);
    }

    /**
     * Reads each line from the file provided and stores for each line the ReplicaAddress and the stub channel in a pair
     * of the list replicas
     * @param configFilePath absolute path to the configuration text file with the replicas' addresses
     * @throws IOException in case an I/O error occurs
     */
    private static void readConfigFile(String configFilePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(configFilePath))) {
            String address;
            ReplicaAddress replicaAddress;
            while ((address = br.readLine()) != null) {
                replicaAddress = new ReplicaAddress(address);
                replicas.add(
                        new Pair<>(
                                replicaAddress,
                                replicaId != replicas.size() ? initStub(replicaAddress) : null // To not initiate a client stub to itself
                        )
                );
            }
        }
    }

    /**
     * Initializes the thread responsible for waiting for the results to arrive and then processes the results obtained
     * @return the results thread instance
     */
    private static Thread initResultsThread() {
        Thread resultsThread = new Thread(() -> {
            Result result;
            int observedValue;
            while(true) {
                try {
                    result = blockingQueue.take();
                    observedValue = waitingResults.get();
                    if (waitingResults.compareAndSet(observedValue, --observedValue) && observedValue > 0) {
                        System.out.println("ResultMessage from " + result.getId() + ": " + result.getResultMessage() + "\n");
                        continue;
                    }
                    // If the timestamps are different, then a result from a previous request arrived
                    if (!lastRequestTimestamp.get().equals(result.getTimestamp())) continue;
                    lastRequestTimestamp.set(null);
                    if (result.hasResults()) {
                        System.out.println("ResultList: " + result.getResults().getListList() + "\n");
                        continue;
                    }
                    System.out.println("ResultMessage: " + result.getResultMessage() + "\n");
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
     * @param id current replica's id
     * @param configFilePath absolute path to the configuration text file with the replicas' address
     * @throws IOException in case an I/O error occurs while reading the file
     */
    private static void initReplica(int id, String configFilePath) throws IOException {
        replicaId = id;
        readConfigFile(configFilePath);
        eventLogic = new EventLogic();
        waitingResults = new AtomicInteger(0);
        lastRequestTimestamp = new AtomicReference<>(null);
        serverThread = GRPCServer.initServerThread(replicas.get(replicaId).getFirst().getPort(), eventLogic);
        resultsThread = initResultsThread();
    }

    private static Timestamp getInstantTimestamp() {
        var timestamp = java.sql.Timestamp.from(Instant.now());
        var rpcTimestamp = Timestamp.newBuilder().setSeconds(timestamp.getTime()).setNanos(timestamp.getNanos()).build();
        lastRequestTimestamp.set(rpcTimestamp);
        return rpcTimestamp;
    }

    private static void quorumInvoke(String requestLabel, String requestData, Timestamp timestamp) {
        for (int id = 0; id < replicas.size(); id++) {
            if (replicaId == id) continue;
            invoke(id, requestLabel, requestData, timestamp);
        }
    }

    /**
     * Menu with all the possible operations to test the system
     */
    private static void operations() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String options = "Choose an operation: \n" +
                    " [0] ADD string to all replicas\n" +
                    " [1] GET set of strings from a replica\n" +
                    " [2] Go back";
            System.out.println(options);
            System.out.print("-> ");

            switch (scanner.nextLine()) {
                case "0": {
                    if (!waitingResults.compareAndSet(0, Math.round(replicas.size() / 2f) - 1)) // TODO: MAL
                        throw new IllegalStateException();
                    quorumInvoke(EventLogic.Events.ADD.name(),  scanner.nextLine(), getInstantTimestamp());
//                    invoke(1, EventLogic.Events.ADD.name(), scanner.nextLine(), getInstantTimestamp());
                    break;
                }
                case "1": {
                    if (!waitingResults.compareAndSet(0, 1)) throw new IllegalStateException();
                    invoke(scanner.nextInt(), EventLogic.Events.GET.name(), "",getInstantTimestamp());
                    break;
                }
                default: return;
            }
        }
    }

    private static void invoke(int replicaId, String requestLabel, String requestData, Timestamp timestamp) {
        var serverStreamObserver = replicas.get(replicaId).getSecond().invoke(new ClientStreamObserver(blockingQueue));
        serverStreamObserver.onNext(
                Request.newBuilder()
                        .setId(replicaId)
                        .setLabel(requestLabel)
                        .setData(requestData)
                        .setTimestamp(timestamp)
                        .build()
        );
    }

    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.out.println("Usage: java -jar Replica.jar <id(>= 0)> <configFile(absolute path)>");
                System.exit(-1);
            }
//            args[0] = auxToDelete();
            initReplica(Integer.parseInt(args[0]), args[1]);
            System.out.println(Arrays.toString(replicas.toArray()));
            operations();
        } catch (IOException e) {
            System.out.println("* ERROR * " + e);
        }
    }

    //apenas para poder criar varias instancias sem ter que estar a alterar o argument 0.
    public static String auxToDelete() {
        Scanner scanner = new Scanner(System.in);
        return scanner.nextLine();
    }
}
