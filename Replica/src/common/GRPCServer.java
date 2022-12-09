package common;

import events.AppendEntriesEvent;
import events.IncreaseByEvent;
import events.EventHandler;
import events.EventLogic;
import events.models.AppendEntriesRPC;
import events.models.LogElement;
import events.models.State;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import replica.Request;
import replica.Result;
import replica.ServerGrpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Optional;

import static events.IncreaseByEvent.OPERATIONS;

public class GRPCServer extends ServerGrpc.ServerImplBase {
    private static Server svc;
    private static Thread serverThread;
    private final EventLogic eventLogic;

    private final State state;

    public GRPCServer(EventLogic eventLogic, State state) {
        this.state = state;
        this.eventLogic = eventLogic;
    }

    @Override
    public void request(Request request, StreamObserver<Result> responseObserver) {
        System.out.println("---> REQUEST BY CLIENT RECEIVED");

        try {

            if (state.getCurrentState() != State.ReplicaState.LEADER) { // If received request while not being the leader, inform the client of the current leader
                responseObserver.onNext(Result.newBuilder().setId(state.getCurrentLeader()).build());
                responseObserver.onCompleted();
                return;
            }

            if (!OPERATIONS.contains(request.getLabel())) {
                throw new Exception();
            }
            Replica.blockingQueueClient.add(request);
            System.out.println("Send response to the client");
            //TODO: PENSAR MELHOR NO QUE É QUE SE VAI DEVOLVER AO CLIENTE
            responseObserver.onNext(Result.newBuilder().setId(state.getCurrentLeader()).build());
            responseObserver.onCompleted();

            // enviar add to log
            // incrementar a coisa
            // enviar commit
            // retornar alguma informaçao
        } catch (Exception e) {
            Throwable th = new StatusException(Status.ABORTED.withDescription("There was a problem"));
            responseObserver.onError(th);
        }
    }

    @Override
    public void invoke(Request request, StreamObserver<Result> responseObserver) {
        Optional<EventHandler> handler = eventLogic.getEventHandler(request.getLabel());
        if (handler.isEmpty()) {
            String message = "The requested label '" + request.getLabel() + "' is not valid.";
            responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(message)));
            return;
        }
        responseObserver.onNext(handler.get().processRequest(
                request.getId(),
                request.getLabel(),
                request.getData(),
                request.getTimestamp())
        );
        responseObserver.onCompleted();
    }

    //TODO: implementar o request, que será chamado pelo client.
    // -> se o server n for o lider, informa o client quem é.
    // -> se for o lider, vai adicionar ao log, e enviar para os restantes followers,
    // quando a maioria receber o pedido do client, o server vai dar commit do log, vai
    // afetar a state machine, irá informar os restantes que realizou o commit e que
    // também o podem realizar e informará o cliente que o pedido foi processado com exito.

    /**
     * Initializes the thread responsible for receiving the requests sent by the others replicas
     * @param port port number for the server
     * @return the server thread instance
     */
    public static Thread initServerThread(int port, EventLogic eventLogic, State state) {
        serverThread = new Thread(() -> {
            svc = ServerBuilder
                    .forPort(port)
                    .addService(new GRPCServer(eventLogic, state))
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
