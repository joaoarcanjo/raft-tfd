package streamobservers;

import io.grpc.stub.StreamObserver;
import replica.Result;

import java.util.concurrent.BlockingQueue;

public class ClientStreamObserver implements StreamObserver<Result> {
    private final BlockingQueue<Result> blockingQueue;

    public ClientStreamObserver(BlockingQueue<Result> blockingQueue) {
        this.blockingQueue = blockingQueue;
    }

    @Override
    public void onNext(Result result) {
        blockingQueue.add(result);
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println("* ERROR * " + throwable.getMessage());
    }

    @Override
    public void onCompleted() {

    }
}
