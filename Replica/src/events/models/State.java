package events.models;

import utils.FileManager;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

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
    private final String logFile;

    //The next index present to be committed. Basically is the lastApplied + 1.
    private int commitIndex;
    //Number of entries present in the file plus the number of committed entries present in the list
    private int lastApplied;

    // For each server, index of the next log entry to send to that server.
    private LinkedList<Integer> nextIndex;
    private LinkedList<Integer> matchIndex;
    private final StateMachine stateMachine;

    public State(int term, int replicaId) {
        currentTerm = term;
        votedFor = -1;
        commitIndex = -1;
        lastApplied = -1;
        lastLogTerm = -1;
        listLogsCommitted = 0;
        logFile = "log" + replicaId + ".txt";
        log = new LinkedList<>();
        currentState = ReplicaState.FOLLOWER;
        stateMachine = new StateMachine();
    }

    //Called after election
    public void InitLeaderState(int numberOfReplicas) {
        nextIndex = new LinkedList<>();
        matchIndex = new LinkedList<>();
        for (int i = 0; i < numberOfReplicas; i++) {
            nextIndex.add(log.size());
            matchIndex.add(0);
        }
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
        lastLogTerm = element.getTerm();
        System.out.println("LOG: New log added!");
        System.out.println(" ---> label: " + element.getLabel());
        System.out.println(" ---> args: " + ByteBuffer.wrap(element.getCommandArgs()).getInt());
        log.add(element);
    }

    public void deleteUncommittedLogs() {
        while(log.size() >= listLogsCommitted) {
            log.remove();
        }
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public int getCurrentLeader() {
        return currentLeader;
    }

    public void setCurrentLeader(int currentLeader) {
        this.currentLeader = currentLeader;
    }

    public void incCommitIndex() {
        ++listLogsCommitted;
        ++commitIndex;
    }
    public int getCommitIndex() { return commitIndex; }
    public int getLastApplied() { return lastApplied; }
    public void incLastApplied() {
        ++lastApplied;
    }
    public void setNextIndex(int replicaId, int index) {
        this.nextIndex.set(replicaId, index);
    }
    public void incMatchIndex(int replicaId) {
        this.matchIndex.set(replicaId, matchIndex.get(replicaId) + 1);
    }

    /**
     * This function will affect the last entry os logs to the state machine
     */
    public void updateStateMachine() {
        if(commitIndex == -1) return; //if it doesn't exist any entry to commit
        incLastApplied();
        LogElement.LogElementArgs element = log.peekLast();
        stateMachine.decodeLogElement(element);
    }

    public LinkedList<LogElement.LogElementArgs> getEntries(int replicaId) {
        if(nextIndex.isEmpty()) return null;
        //index is the next index to send to the follower.
        int index = nextIndex.get(replicaId);
        System.out.println("ReplicaId: " + replicaId + "index: " + index);
        LinkedList<LogElement.LogElementArgs> entries = new LinkedList<>();
        //se a diferença entre o index e o commitIndex for superior à lista de logs, temos que
        //ir buscar alguns ao ficheiro. Ou seja, se o index for 20, e o commitIndex for 35, quer dizer
        //que o lider tem que enviar 15 entries. Por exemplo, caso a lista apenas contenha 10 entries,
        //temos que ir ao ficheiro buscar as últimas 5 linhas, para podermos enviar as 15 que o follower necessita.

        //se a seguinte variavel possuir um valor superior a 0, é o número de linhas a ir buscar ao ficheiro
        //se o número for 0, basta retornar toda a lista log.
        //se o número for inferior a 0, quer dizer que as entries necessarias estao todas na log, é ir buscar as ultimas commitIndex-index
        int linesToReadFromFile = (commitIndex - index) - log.size();
        if (linesToReadFromFile == 0) return log;
        if (linesToReadFromFile < 0) {
            for (int i = log.size() - (commitIndex - index); i < log.size(); i++) {
                entries.add(log.get(i));
            }
            return entries;
        }
        FileManager.readLastNLines(logFile, linesToReadFromFile)
                .forEach(line -> entries.add(LogElement.jsonToLogElement(line)));
        entries.addAll(log);
        return entries;
    }
}
