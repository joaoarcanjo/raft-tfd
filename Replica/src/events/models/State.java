package events.models;

import utils.FileManager;
import utils.Pair;
import java.nio.ByteBuffer;
import java.util.*;

public class State {
    public enum ReplicaState {
        FOLLOWER, CANDIDATE, LEADER
    }
    private int currentLeader;
    private int currentTerm;
    private int votedFor;
    private ReplicaState currentState;
    //Command and term
    private LinkedList<LogElement.LogElementArgs> log;
    //Number of committed entries present in the log list
    private int listLogsCommitted;
    private final int COMMITS_TO_FILE = 2;
    private Pair<Integer, Integer> nextLimitsToFile;
    //The term of the last log
    private int lastLogTerm;
    //Number of logs (committed e uncommitted).
    private int lastLogIndex;
    private int numberOfFileLines;
    private final String logFile;

    //The next index present to be committed.
    private int commitIndex;
    //Number of entries present in the file plus the number of committed entries present in the list
    private int lastApplied;

    // For each server, index of the next log entry to send to that server.
    private LinkedList<Integer> nextIndex;
    private LinkedList<Integer> matchIndex;
    private final StateMachine stateMachine;

    public State(int term, int replicaId) {
        logFile = "log" + replicaId + ".txt";
        numberOfFileLines = FileManager.getNumberOfLines(logFile);
        currentTerm = term;
        votedFor = -1;
        commitIndex = numberOfFileLines - 1;
        lastApplied = numberOfFileLines - 1;
        lastLogIndex = numberOfFileLines - 1;
        lastLogTerm = initLastLogTerm();
        listLogsCommitted = 0;
        currentState = ReplicaState.FOLLOWER;
        stateMachine = new StateMachine();
        initLog();
        nextLimitsToFile = new Pair<>(-1, -1);
    }
    public void listCommittedLogs() {
        System.out.println("-- List of committed logs --");
        System.out.println("-> Last applied: " + lastApplied);
        System.out.println("-> listLogsCommitted: " + listLogsCommitted);
        for (int i = 0; i <= lastApplied; i++) {
            int arg = ByteBuffer.wrap(log.get(i).getCommandArgs()).getInt();
            System.out.println("- Entry Committed: " + arg);
        }
    }
    public void listAllLogs() {
        System.out.println("-- List of all logs --");
        for (int i = 0; i <= lastLogIndex; i++) {
            int arg = ByteBuffer.wrap(log.get(i).getCommandArgs()).getInt();
            System.out.println("- Entry of log: " + arg);
        }
    }
    public void initLog() {
        log = new LinkedList<>();
        if(numberOfFileLines > 0) {
            LinkedList<String> lines = FileManager.readLastNLines(logFile, numberOfFileLines);
            lines.forEach(line -> {
                LogElement.LogElementArgs element = LogElement.jsonToLogElement(line);
                int arg = ByteBuffer.wrap(element.getCommandArgs()).getInt();
                System.out.println(arg);
                log.add(element);
                stateMachine.decodeLogElement(element);
            });
        }
        System.out.println("-> State of machine: " + stateMachine.getCounter()  + " <-\n");
    }

    private int initLastLogTerm() {
        LinkedList<String> lines = FileManager.readLastNLines(logFile, 1);
        if(lines.isEmpty()) return 0;
        return LogElement.jsonToLogElement(lines.getLast()).getTerm();
    }
    public void initLeaderState(int numberOfReplicas) {
        nextIndex = new LinkedList<>();
        matchIndex = new LinkedList<>();
        for (int i = 0; i < numberOfReplicas; i++) {
            nextIndex.add(lastLogIndex + 1);
            matchIndex.add(0);
        }
        System.out.println("-> Next index of each replica: " +nextIndex);
    }
    private void incLastLogIndex() {
        lastLogIndex++;
    }
    public void updateCommitIndexLeader() {
        PriorityQueue<Integer> committedIndex = new PriorityQueue<>((o1, o2) -> {
            if (Objects.equals(o1, o2)) return 0;
            return o1 > o2 ? -1 : +1;
        });
        committedIndex.addAll(nextIndex);
        int majority = nextIndex.size() / 2 + 1;
        int commitIndexAux = commitIndex;
        while (committedIndex.peek() != null && --majority >= 0) {
            commitIndexAux = committedIndex.poll() - 1;
        }

        if (commitIndex != commitIndexAux) {
            commitIndex = commitIndexAux;
        }
        updateStateMachine();
    }
    public void setCommitIndex(int index) {
        int numberOfNewCommitted = index - (numberOfFileLines - 1 + listLogsCommitted);
        listLogsCommitted += numberOfNewCommitted;
        commitIndex = index;
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
    public void addToLog(LogElement.LogElementArgs element) {
        log.add(element);
        incLastLogIndex();
        lastLogTerm = element.getTerm();
    }
    public void deleteUncommittedLogs() {
        if (lastLogIndex == -1) return;
        int aux = lastLogIndex;
        int deletedLogs = 0;
        while(aux-- > lastApplied) {
            lastLogIndex--;
            deletedLogs++;
            log.removeLast();
        }
        if(listLogsCommitted > 0) {
            lastLogTerm = log.get(lastLogIndex).getTerm();
        } else {
            initLastLogTerm();
        }
        System.out.println("--> Number of logs deleted: " + deletedLogs + " <--");
    }
    public int getLastLogTerm() {
        return lastLogTerm;
    }
    public int getLastLogIndex() {
        return lastLogIndex;
    }
    public int getCurrentLeader() {
        return currentLeader;
    }
    public void setCurrentLeader(int currentLeader) {
        this.currentLeader = currentLeader;
    }
    public void incCommitIndex() {
       /* ++listLogsCommitted;
        ++commitIndex;*/
    }
    public int getCommitIndex() { return commitIndex; }
    public int getLastApplied() { return lastApplied; }
    public void incLastApplied() {
        ++lastApplied;
    }
    public void incMatchIndex(int replicaId) {
        matchIndex.set(replicaId, matchIndex.get(replicaId) + 1);
    }
    public void setNextIndex(int replicaId, int index) {
        nextIndex.set(replicaId, index);
    }
    public void incNextIndex(int replicaId) {
        nextIndex.set(replicaId, nextIndex.get(replicaId) + 1);
    }
    public int getNextIndex(int replicaId) {
        return nextIndex.get(replicaId);
    }

    /**
    /**
     * This function will affect the last entry of logs to the state machine
     */
    public void updateStateMachine() {
        if(lastLogIndex == -1 || commitIndex == -1 || commitIndex <= lastApplied) return; //if it doesn't exist any entry to commit
        System.out.println("\n$ Commit index: " + commitIndex + " $");
        System.out.println("--> Updating state machine <--");
        while(commitIndex < log.size() && commitIndex > lastApplied) {
            int toApplyIndex = lastApplied + 1;
            LogElement.LogElementArgs element = log.get(toApplyIndex);
            stateMachine.decodeLogElement(element);
            incLastApplied();
        }
        updateLimits();
        System.out.println("-> New state of machine: " + stateMachine.getCounter()  + " <-\n");
    }
    public void updateLimits() {
        if(nextLimitsToFile.getFirst() == -1) {
            nextLimitsToFile.setFirst(lastApplied);
        }
        nextLimitsToFile.setSecond(lastApplied);
        if((nextLimitsToFile.getSecond() - nextLimitsToFile.getFirst()) >= COMMITS_TO_FILE - 1) {
            for (int i = nextLimitsToFile.getFirst(); i <= nextLimitsToFile.getSecond(); i++) {
                String line = LogElement.logElementToJson(log.get(i));
                FileManager.addLine(logFile, LogElement.logElementToJson(log.get(i)));
                System.out.println("Line added: " + line);
            }
            nextLimitsToFile.setFirst(-1);
            nextLimitsToFile.setSecond(-1);
        }
    }
    public LogElement.LogElementArgs getEntry(int index) {
        if(index < 0) return null;
        return log.get(index);
    }
    public LinkedList<LogElement.LogElementArgs> getEntries(int replicaId) {
        int index = nextIndex.get(replicaId);
        //System.out.println("ReplicaId: " + replicaId + ", index: " + index);
        LinkedList<LogElement.LogElementArgs> entries = new LinkedList<>();
        if (log.size() > 0) {
            //System.out.println("Index replica: " + index);
            //System.out.println("lastLogIndex replica: " + lastLogIndex);
            for (int i = index; i <= lastLogIndex; i++) {
                entries.add(log.get(i));
            }
            return entries;
        }
        return entries;
    }
}
