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
    private static EventLogic eventLogic;

    public static final BlockingQueue<Result> blockingQueue = new LinkedBlockingQueue<>();

    private static boolean terminate = false;
    private static AtomicInteger waitingResults;
    private static AtomicReference<Timestamp> lastRequestTimestamp;
    private static int replicaId;

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
                    result = blockingQueue.take();

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
        readConfigFile(configFilePath);
        eventLogic = new EventLogic();
        waitingResults = new AtomicInteger(0);
        lastRequestTimestamp = new AtomicReference<>(null);
        GRPCServer.initServerThread(replicas.get(replicaId).getFirst().getPort(), eventLogic);
        initResultsThread();
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
            System.out.println("ResultList: " + eventLogic.getReceivedData());
            resetRequestTimestamp();
            waitingResults.set(0);
            return;
        }

        Request request = Request.newBuilder().setId(destinyReplicaId).setLabel(requestLabel).setData(requestData).setTimestamp(timestamp).build();
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

        // If the operation is an ADD, then add the data to the Replica itself
        if (Objects.equals(requestLabel, EventLogic.Events.ADD.name())) {
            eventLogic.receivedData.add(requestData);
        }

        for (int id = 0; id < replicas.size(); id++) {
            if (replicaId == id) continue;
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
        String options = "Choose an operation: \n" + " [0] ADD string to all replicas\n" + " [1] GET set of strings from a replica\n" + " [2] Exit";
        while (!terminate) {
            System.out.println(options);
            System.out.print("-> ");

            switch (scanner.nextLine()) {
                case "0": {
                    if (!waitingResults.compareAndSet(0, Math.round(replicas.size() / 2f))) { // k > n/2 - 1
                        System.out.println(" * Still waiting for the majority of results...");
                        cancelOperation(scanner);
                        break;
                    }
                    System.out.print("Insert the data: \n-> ");
                    String data = scanner.nextLine();

                    Timestamp rpcTimestamp = getInstantTimestamp();
                    lastRequestTimestamp.set(rpcTimestamp);
                    quorumInvoke(EventLogic.Events.ADD.name(), data, rpcTimestamp);
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

                    while(id >= replicas.size()) {
                        System.out.println("Please provide an Id that exists");
                        System.out.print("Replica id: \n-> ");
                        id = Integer.parseInt(scanner.nextLine());
                    }

                    Timestamp rpcTimestamp = getInstantTimestamp();
                    lastRequestTimestamp.set(rpcTimestamp);
                    invoke(id, EventLogic.Events.GET.name(), "", rpcTimestamp);
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

    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.out.println("Usage: java -jar Replica.jar <id(>= 0)> <configFile(absolute path)>");
                System.exit(-1);
            }
            initReplica(Integer.parseInt(args[0]), args[1]);
            System.out.println(" * REPLICA ID: " + replicaId + " *");
            operations();
        } catch (IOException e) {
            System.out.println("* ERROR * " + e);
        }
    }
}
