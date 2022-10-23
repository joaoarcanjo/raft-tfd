import events.EventHandler;
import events.EventLogic;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import replica.Request;
import replica.Result;
import replica.ServerGrpc;

import java.io.IOException;
import java.util.Optional;

public class GRPCServer extends ServerGrpc.ServerImplBase {

    private static Server svc;
    private static Thread serverThread;
    private final EventLogic eventLogic;

    public GRPCServer(EventLogic eventLogic) {
        this.eventLogic = eventLogic;
    }

    @Override
    public void invoke(Request request, StreamObserver<Result> responseObserver) {
        Optional<EventHandler> handler = eventLogic.getEventHandler(request.getLabel());
        if (handler.isEmpty()) {
            String message = "The requested label '" + request.getLabel() + "' is not valid.";
            responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(message)));
            return;
        }
        responseObserver.onNext(handler.get().processRequest(request.getId(), request.getLabel(), request.getData(), request.getTimestamp()));
        responseObserver.onCompleted();
    }

    /**
     * Initializes the thread responsible for receiving the requests sent by the others replicas
     * @param port port number for the server
     * @return the server thread instance
     */
    public static Thread initServerThread(int port, EventLogic eventLogic) {
        serverThread = new Thread(() -> {
            svc = ServerBuilder
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

    public static boolean terminateServerThread() {
        svc.shutdown();
        try {
            serverThread.join();
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }
}
