package org.example;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import replica.Request;
import replica.Result;
import replica.ServerGrpc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Client {
    private static final int MINIMUM = 1;
    private static final int MAXIMUM = 6;
    private static final int INT_SIZE = 4;
    public static final int CLIENT_ID = -1;
    public static final int WAITING_TIME = 5000;
    private static final String INCREASE_LABEL = "increaseBy";

    private static int current_leader = 2;

    private static final List<Pair<ReplicaAddress, ServerGrpc.ServerBlockingStub>> replicas = new ArrayList<>();

    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.println("Usage: java -jar Client.jar <configFile(absolute path)>");
            System.exit(-1);
        }

        try {
            initClient(args[0]);
            sendCommands();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void sendCommands() {
        Request request = createRequestMessage();
        boolean reset = false;
        while (true) {
            try {
                System.out.println("Sending command to replica " + current_leader);
                Result response = replicas.get(current_leader).getSecond().request(request);

                System.out.println("Response arrived: " + response.getResultMessage());

                if (response.getId() == -1) {
                    return;
                }
                if (response.getId() != current_leader) {
                    current_leader = response.getId();
                    System.out.println("Switched leader to: " + current_leader);
                } else {
                    Thread.sleep(WAITING_TIME);
                    request = createRequestMessage();
                }
            } catch (Exception e){
                if(current_leader > 0 && !reset) {
                    current_leader = 0;
                    reset = true;
                } else {
                    current_leader++;
                    if (current_leader == replicas.size()) {
                        System.out.println("No available replicas to be found, exiting..");
                        System.exit(-1);
                    }
                }
                System.out.println("No reply from replica or crash occurred, attempting communication with other known replica (" + current_leader + ")");
            }

        }
    }

    private static Request createRequestMessage() {
        System.out.println("Create request message called");
        Random rand = new Random();
        int value = rand.nextInt(MAXIMUM - MINIMUM) + MINIMUM;
        System.out.println("Random value generated: "+ value);
            byte[] data = ByteBuffer.allocate(INT_SIZE).putInt(value).array();
        return Request.newBuilder()
                .setId(CLIENT_ID)
                .setLabel(INCREASE_LABEL)
                .setData(ByteString.copyFrom(data))
                .setTimestamp(getInstantTimestamp())
                .build();
    }

    private static Timestamp getInstantTimestamp() {
        var timestamp = java.sql.Timestamp.from(Instant.now());
        return Timestamp.newBuilder().setSeconds(timestamp.getTime()).setNanos(timestamp.getNanos()).build();
    }

    private static void initClient(String configFilePath) throws IOException {
        readConfigFile(configFilePath);
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
                replicas.add(new Pair<>(replicaAddress, initStub(replicaAddress)) // To not initiate a client stub to itself
                );
            }
        }
    }

    /**
     * Initializes the client communication channel
     *
     * @param replicaAddress address of the replica to establish the connection
     * @return the stub created
     */
    private static ServerGrpc.ServerBlockingStub initStub(ReplicaAddress replicaAddress) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(replicaAddress.getIp(), replicaAddress.getPort()).usePlaintext().build();
        return ServerGrpc.newBlockingStub(channel);
    }
}