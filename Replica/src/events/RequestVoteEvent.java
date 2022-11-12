package events;

import com.google.protobuf.Timestamp;
import events.models.RequestVoteRPC;
import events.models.State;
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

        boolean vote = false; // Starts at false, because it will only turn true if there's a term superior to ours

        if(state.getVotedFor() == -1 || requestVoteArgs.term > state.getCurrentTerm()) {
            vote = true;
            state.setCurrentTerm(requestVoteArgs.term);
            state.setVotedFor(requestVoteArgs.candidateId);
        }

        return Result.newBuilder().setResultMessage(RequestVoteRPC.resultVoteToJson(state.getCurrentTerm(), vote)).build();
    }

    @Override
    public void processSelfRequest(String label, String data) {
        state.setVotedFor(RequestVoteRPC.requestVoteArgsFromJson(data).candidateId);
    }
}
