import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import replica.Request;
import replica.Result;
import replica.ServerGrpc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Replica {
    /**
     * Each position of the list correspond to an id of a replica, which contains its socket address
     */
    private static final List<ReplicaAddress> replicasAddresses= new ArrayList<>();
    private static final HashMap<Integer, ServerGrpc.ServerStub> replicasStubs= new HashMap<>();
    private static final ClientStreamObserver clientStreamObserver = new ClientStreamObserver();
    private static final HashSet<String> replicaMessages = new HashSet<>();
    public static final LinkedBlockingQueue<Result> blockingQueue = new LinkedBlockingQueue<>();

    private static final AtomicBoolean isWaiting = new AtomicBoolean(false);
    private static int replicaId;

    /**
     * Initializes the initial properties of a replica, i.e. its id and the addresses of the other replicas
     * @param id current replica's id
     * @param configFilePath absolute path to the configuration text file with the replicas' address
     * @throws IOException in case an I/O error occurs while reading the file
     */
    private static void initReplica(int id, String configFilePath) throws IOException {
        replicaId = id;
        readConfigFile(configFilePath);
    }

    /**
     * Reads each line from the file provided and stores for each line the ServerAddress in the replicasAddresses list
     * @param configFilePath absolute path to the configuration text file with the replicas' addresses
     * @throws IOException in case an I/O error occurs
     */
    private static void readConfigFile(String configFilePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(configFilePath))) {
            String address;
            while ((address = br.readLine()) != null) {
                replicasAddresses.add(new ReplicaAddress(address));
            }
        }
    }

    private static void runReplica() throws IOException {
        System.out.println(replicasAddresses);

        Thread listener = new Thread(() -> {
            Server svc = ServerBuilder
                    .forPort(replicasAddresses.get(replicaId).getPort())
                    .addService(new GRPCServer())
                    .build();
            try {
                svc.start();
                svc.awaitTermination();
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        });
        listener.start();
        //listener.join();

        stubsInit();
        Thread analyserThread = analyseQueue();
        menu(analyserThread);
        // Chamar a operaçao

        // Ficar a aguardar a resposta

        // Imprimir a resposta

        // Voltar para o início do ciclo
    }

    /**
     * Starts the communication channels and saves them for use in future communications.
     */
    private static void stubsInit() {
        ReplicaAddress replicaAddress;
        ManagedChannel channel;
        ServerGrpc.ServerStub noBlockStub;

        for (int currentId = 0; currentId < replicasAddresses.size(); currentId++) {
            if(currentId == replicaId) continue;
            replicaAddress = replicasAddresses.get(currentId);
            channel = ManagedChannelBuilder
                    .forAddress(replicaAddress.getIp(), replicaAddress.getPort())
                    .usePlaintext()
                    .build();
            noBlockStub = ServerGrpc.newStub(channel);
            replicasStubs.put(currentId, noBlockStub);
        }
    }

    private static void menu(Thread analyserThread) {
        Scanner scanner = new Scanner(System.in);
        boolean stopInstance = false;

        String menuOptions =
                "Choose an operation: \n" +
                " -> [0] Invoke instance\n" +
                " -> [1] Quorum invoke\n" +
                " -> [2] Stop instance"; //option not working

        while (!stopInstance) {
            System.out.println(menuOptions);

            switch (scanner.nextLine()) {
                case "0": {
                    if(isWaiting.get()) {
                        System.out.println("Impossible option, replica is still waiting...");
                        continue;
                    }
                    invokeParameters(scanner);
                }
                case "1": break;
                case "2": {
                    //analyserThread.interrupt();
                    stopInstance = true;
                }
            }
        }
    }

    private static void invokeParameters(Scanner scanner) {
        System.out.println("Insert the id of the replica you want to invoke:");
        int replicaId = Integer.parseInt(scanner.nextLine());
        System.out.println("Insert the request label:");
        String requestLabel = scanner.nextLine();

        String requestData = "";
        if(requestLabel.equals("ADD")) {
            System.out.println("Insert the request data:");
            requestData = scanner.nextLine();
        }
        invoke(replicaId, requestLabel, requestData);
    }

    private static void invoke(int replicaId, String requestLabel, String requestData) {
        StreamObserver<Request> serverStreamObserver = replicasStubs.get(replicaId).invoke(clientStreamObserver);

        serverStreamObserver.onNext(
                Request.newBuilder()
                        .setId(String.valueOf(Replica.replicaId))
                        .setLabel(requestLabel)
                        .setData(requestData)
                        .build()
        );

        isWaiting.set(true);
    }

    private static Thread analyseQueue() {
        Thread analyser = new Thread(() -> {
            while(true) {
                Result result;
                try {
                    result = blockingQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (result.hasResults()) {
                    System.out.println("ResultList: " + result.getResults().getListList() + "\n");
                } else {
                    System.out.println("Ack: " + result.getAck() + "\n");
                }
                isWaiting.set(false);
            }
        });
        analyser.start();
        return analyser;
    }

    public static boolean addMessage(String message) {
        return replicaMessages.add(message);
    }

    public static HashSet<String> getReplicaMessages() {
        return replicaMessages;
    }

    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.out.println("Usage: java -jar Replica.jar <id(>= 0)> <configFile(absolute path)>");
                System.exit(-1);
            }
            args[0] = auxToDelete();
            initReplica(Integer.parseInt(args[0]), args[1]);
            runReplica();
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
