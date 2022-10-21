package streamobservers;

import events.EventHandler;
import events.EventLogic;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import replica.Request;
import replica.Result;

import java.util.Optional;

public class ServerStreamObserver implements StreamObserver<Request> {

    private final EventLogic eventLogic;
    private final StreamObserver<Result> resultStream;

    public ServerStreamObserver(StreamObserver<Result> resultStream, EventLogic eventLogic) {
        this.resultStream = resultStream;
        this.eventLogic = eventLogic;
    }

    @Override
    public void onNext(Request request) {
        Optional<EventHandler> handler = eventLogic.getEventHandler(request.getLabel());
        if (handler.isEmpty()) {
            String message = "The requested label '" + request.getLabel() + "' is not valid.";
            resultStream.onError(new StatusException(Status.INVALID_ARGUMENT.withDescription(message)));
            return;
        }
        resultStream.onNext(handler.get().processRequest(request.getId(), request.getLabel(), request.getData(), request.getTimestamp()));
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println("* ERROR * " + throwable.getMessage());
    }

    @Override
    public void onCompleted() {}
}
