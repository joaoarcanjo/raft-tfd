package events;

import com.google.protobuf.Timestamp;
import events.objects.RequestVoteRPC;
import events.objects.State;
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

        if(requestVoteArgs.term < state.getCurrentTerm()) {
            return Result.newBuilder().setResultMessage(String.valueOf(state.getCurrentTerm())).build();
        }

        if(state.getVotedFor() == -1 || requestVoteArgs.term > state.getCurrentTerm()) {
            state.setCurrentTerm(requestVoteArgs.term);
            state.setVotedFor(requestVoteArgs.candidateId);
            return Result.newBuilder().setResultMessage(String.valueOf(true)).build();
        }

        return Result.newBuilder().setResultMessage(String.valueOf(false)).build();
    }

    @Override
    public void processSelfRequest(String label, String data) {
        state.setVotedFor(RequestVoteRPC.requestVoteArgsFromJson(data).candidateId);
    }
}
