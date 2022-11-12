package events;

import com.google.protobuf.Timestamp;
import replica.Result;

public class RequestVoteEvent implements EventHandler {
    public static final String LABEL = "VOTE";
    private final State state;

    public RequestVoteEvent(State state) {
        this.state = state;
    }

    @Override
    public Result processRequest(int senderId, String label, String data, Timestamp timestamp) {
        RequestVoteRPC.RequestVoteArgs requestVoteArgs = RequestVoteRPC.requestVoteArgsFromJson(data);
        //vou verificar o term do pedido
        return null;
    }

    @Override
    public void processSelfRequest(String label, String data) {

    }
}
