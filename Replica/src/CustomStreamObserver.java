import io.grpc.stub.StreamObserver;
import replica.Request;
import replica.Result;

public class CustomStreamObserver implements StreamObserver<Request> {

    private final ServletLogic logicHandler;
    private final StreamObserver<Result> resultStream;

    public CustomStreamObserver(StreamObserver<Result> resultStream, ServletLogic logicHandler) {
        this.resultStream = resultStream;
        this.logicHandler = logicHandler;
    }

    @Override
    public void onNext(Request request) {
        try {
            resultStream.onNext(logicHandler.processRequest(request.getId(), request.getLabel(), request.getData()));
        } catch (Exception e) {
            resultStream.onError(e);
        }
    }

    @Override
    public void onError(Throwable throwable) {}

    @Override
    public void onCompleted() {}
}
