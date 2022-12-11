package events.models;

import utils.FileManager;

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
    private final LinkedList<LogElement.LogElementArgs> log;
    //Number of committed entries present in the log list
    private int listLogsCommitted;
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
        numberOfFileLines = FileManager.getNumberOfLines(logFile); //TODO: atualizar quando se envia para o ficheiro.
        currentTerm = term;
        votedFor = -1;
        commitIndex = numberOfFileLines;
        lastApplied = numberOfFileLines - 1;
        lastLogIndex = numberOfFileLines - 1;
        lastLogTerm = initLastLogTerm();
        listLogsCommitted = 0;
        log = new LinkedList<>();
        currentState = ReplicaState.FOLLOWER;
        stateMachine = new StateMachine();
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
        while(commitIndex > lastApplied) {
            int toApplyIndex = lastApplied + 1;
            LogElement.LogElementArgs element = log.get(toApplyIndex - numberOfFileLines);
            stateMachine.decodeLogElement(element);
            incLastApplied();
        }
        System.out.println("-> New state of machine: " + stateMachine.getCounter()  + " <-\n");
    }
    public StateMachine getStateMachine() {
        return stateMachine;
    }

    //TODO: nao respeita os casos do ficheiro.
    public LogElement.LogElementArgs getEntry(int index) {
        if(index < 0) return null;
        /*
        int lineToReadFromFile = (lastLogIndex - index) - log.size();
        System.out.println("Lines to read from file: " + lineToReadFromFile);
        if (lineToReadFromFile == 0) return log.get(0);
        if (lineToReadFromFile < 0) {
            int i = log.size() - (commitIndex - index);
            System.out.println("Index from log list: " + i);
            return log.get(i);
        }
        return LogElement.jsonToLogElement(FileManager.readLine(logFile, lineToReadFromFile));*/
        return log.get(index);
    }
    //TODO: nao respeita os casos do ficheiro.
    public LinkedList<LogElement.LogElementArgs> getEntries(int replicaId) {
        //index is the next index to send to the follower.
        int index = nextIndex.get(replicaId);
        //System.out.println("ReplicaId: " + replicaId + ", index: " + index);
        LinkedList<LogElement.LogElementArgs> entries = new LinkedList<>();

        //int linesToReadFromFile = (lastLogIndex - index) - log.size();
        //if (linesToReadFromFile == 0) return log;
        //if (linesToReadFromFile < 0) {
        if (log.size() > 0) {
            //System.out.println("Index replica: " + index);
            //System.out.println("lastLogIndex replica: " + lastLogIndex);
            for (int i = index; i <= lastLogIndex; i++) {
                entries.add(log.get(i));
            }
            return entries;
        }/*
        FileManager.readLastNLines(logFile, linesToReadFromFile)
                .forEach(line -> entries.add(LogElement.jsonToLogElement(line)));
        entries.addAll(log);*/
        return entries;
    }
}
