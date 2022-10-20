import io.grpc.stub.StreamObserver;
import replica.Result;

public class ClientStreamObserver implements StreamObserver<Result> {

    @Override
    public void onNext(Result result) {
        Replica.blockingQueue.add(result);
    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onCompleted() {

    }
}
