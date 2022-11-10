import utils.Pair;

import java.util.LinkedList;

public class State {
    public enum ReplicaState {
        FOLLOWER, CANDIDATE, LEADER
    }

    private int currentTerm;
    private int votedFor;
    private ReplicaState currentState;
    private final LinkedList<Pair<String, Integer>> log;

    public State() {
        currentTerm = 0;
        votedFor = -1;
        log = new LinkedList<>();
        currentState = ReplicaState.FOLLOWER;
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public int getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(int votedFor) {
        this.votedFor = votedFor;
    }

    public ReplicaState getCurrentState() {
        return currentState;
    }

    public void setCurrentState(ReplicaState currentState) {
        this.currentState = currentState;
    }

    public LinkedList<Pair<String, Integer>> getLog() {
        return log;
    }

    public boolean addToLog(String command, int term) {
        return log.add(new Pair<>(command, term));
    }
}
