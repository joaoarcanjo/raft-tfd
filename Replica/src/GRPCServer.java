import events.EventLogic;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import replica.Request;
import replica.Result;
import replica.ServerGrpc;
import streamobservers.ServerStreamObserver;

import java.io.IOException;

public class GRPCServer extends ServerGrpc.ServerImplBase {
    private final EventLogic eventLogic;

    public GRPCServer(EventLogic eventLogic) {
        this.eventLogic = eventLogic;
    }

    @Override
    public StreamObserver<Request> invoke(StreamObserver<Result> resultStream) {
        return new ServerStreamObserver(resultStream, eventLogic);
    }

    /**
     * Initializes the thread responsible for receiving the requests sent by the others replicas
     * @param port port number for the server
     * @return the server thread instance
     */
    public static Thread initServerThread(int port, EventLogic eventLogic) {
        Thread serverThread = new Thread(() -> {
            Server svc = ServerBuilder
                    .forPort(port)
                    .addService(new GRPCServer(eventLogic))
                    .build();
            try {
                svc.start();
                svc.awaitTermination();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        serverThread.start();
        return serverThread;
    }
}
