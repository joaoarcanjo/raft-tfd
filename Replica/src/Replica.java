import io.grpc.Server;
import io.grpc.ServerBuilder;
import replica.ServerGrpc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Replica {
    /**
     * Each position of the list correspond to an id of a replica, which contains its socket address
     */
    private static final List<ReplicaAddress> replicasAddresses= new ArrayList<>();
    private static int replicaId;

    private static ServerGrpc.ServerStub noBlockStub;

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
                    .forPort(8000)
                    .addService(new GRPCServer())
                    .build();
            try {
                svc.start();
                System.out.println("OLA");
                svc.awaitTermination();
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        });
        listener.start();
        //listener.join();

        // Iniciar os canais de comunicação e guardá-los num array

        // -> Criar while infinito
        // Ficar à escuta por input do utilizador, ADD ou GET (talvez ter uma thread sempre à escuta? para por exemplo cancelar)

        // Chamar a operaçao

        // Ficar a aguardar a resposta

        // Imprimir a resposta

        // Voltar para o início do ciclo
    }

    private void invoke() {
        /*noBlockStub.invoke() ->
        noBlockStub.invoke() ->*/
    }

    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.out.println("Usage: java -jar Replica.jar <id(>= 0)> <configFile(absolute path)>");
                System.exit(-1);
            }
            initReplica(Integer.parseInt(args[0]), args[1]);
            runReplica();
        } catch (IOException e) {
            System.out.println("* ERROR * " + e);
        }
    }
}
