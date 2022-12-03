package events;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import events.models.RequestVoteRPC;
import events.models.State;
import replica.Result;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class RequestVoteEvent implements EventHandler {
    public static final String LABEL = "VOTE";
    private final Condition condition;
    private final ReentrantLock monitor;
    private final State state;

    public RequestVoteEvent(ReentrantLock monitor, Condition condition, State state) {
        this.condition = condition;
        this.monitor = monitor;
        this.state = state;
    }

    @Override
    public Result processRequest(int senderId, String label, ByteString data, Timestamp timestamp) {
        RequestVoteRPC.RequestVoteArgs requestVoteArgs = RequestVoteRPC.requestVoteArgsFromJson(data.toString());

        boolean vote = false; // Starts at false, because it will only turn true if there's a term superior to ours

        //System.out.println("--- Current term: " + state.getCurrentTerm() + "; votedFor: " + state.getVotedFor() + " ---");
        //System.out.println("--- Term from request received: " + requestVoteArgs.term + " ---\n");
        System.out.println("\n# Received request vote #");
        if((requestVoteArgs.term == state.getCurrentTerm() && state.getVotedFor() == -1)
                || requestVoteArgs.term > state.getCurrentTerm()) {
            vote = true;
            state.setCurrentTerm(requestVoteArgs.term);
            state.setVotedFor(requestVoteArgs.candidateId);
            state.setCurrentState(State.ReplicaState.FOLLOWER);
            System.out.println("# Voted on " + requestVoteArgs.candidateId + ".\n");

            monitor.lock();
            try {
                condition.signal();
            } finally {
                monitor.unlock();
            }
        } else {
            System.out.println("# Voted false, and update replica " + senderId + " term to: " + state.getCurrentTerm());
        }   // TODO Quando se abre uma quarta ou quinta réplica, não recebe os heartbeats ?! WTF

        return Result.newBuilder()
                .setId(senderId)
                .setResultMessage(RequestVoteRPC.resultVoteToJson(state.getCurrentTerm(), vote))
                .build();
    }

    @Override
    public void processSelfRequest(String label, String data) {
        state.setVotedFor(RequestVoteRPC.requestVoteArgsFromJson(data).candidateId);
    }
}
