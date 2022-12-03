package events.models;

import java.util.Arrays;
import java.util.LinkedList;

public class State {
    public enum ReplicaState {
        FOLLOWER, CANDIDATE, LEADER
    }

    private int currentTerm;
    private int votedFor;
    private ReplicaState currentState;
    //Command and term
    private final LinkedList<LogElement> log;

    //Volatile state on all servers
    private int commitIndex = 0;

    private int lastApplied = 0;

    //Volatile state on leaders, reinitialized after election
    private int[] nextIndex;
    private int[] matchIndex;

    public State(int term) {
        currentTerm = term;
    //public State() {
    //    currentTerm = 0;
        votedFor = -1;
        log = new LinkedList<>();
        currentState = ReplicaState.FOLLOWER;
    }

    //Called after election
    public void InitLeaderState(int numberOfReplicas) {
        nextIndex = new int[numberOfReplicas];
        matchIndex = new int[numberOfReplicas];
        Arrays.fill(nextIndex, log.size()); //Initialized to leader last log index + 1
        Arrays.fill(matchIndex, 0); //Initialized to 0, increases monotonically
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void incCurrentTerm() {
        ++currentTerm;
    }

    public void setCurrentTerm(int newTerm) { currentTerm = newTerm; }

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

    public LinkedList<LogElement> getLog() {
        return log;
    }

    public boolean addToLog(LogElement element) {
        return log.add(element);
    }

    public int getLastLogIndex() {
        return log.size();
    }

    public int getLastLogTerm() {
        if(log.isEmpty()) return -1;
        return log.getLast().getTerm();
    }
    public void incCommitIndex() {
        ++commitIndex;
    }
    public void incLastApplied() {
        ++lastApplied;
    }
    public void setNextIndex(int replicaId, int index) {
        this.nextIndex[replicaId] = index;
    }
    public void incMatchIndex(int replicaId) {
        this.matchIndex[replicaId]++;
    }
}
