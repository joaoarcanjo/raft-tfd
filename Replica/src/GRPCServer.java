import io.grpc.stub.StreamObserver;
import replica.Request;
import replica.Result;
import replica.ServerGrpc;

public class GRPCServer extends ServerGrpc.ServerImplBase {

    @Override
    public StreamObserver<Request> invoke(StreamObserver<Result> responseObserver) {
        // Obter os valores
        // Chamar o EventHandler correspondente
        // E enviar a resposta
        return super.invoke(responseObserver);
    }
}
