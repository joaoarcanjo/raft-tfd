package events.models;

import common.Replica;
import replica.Result;
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
    private int initLastLogTerm() {
        LinkedList<String> lines = FileManager.readLastNLines(logFile, 1);
        if(lines.isEmpty()) return 0;
        return LogElement.jsonToLogElement(lines.getLast()).getTerm();
    }
    public void InitLeaderState(int numberOfReplicas) {
        nextIndex = new LinkedList<>();
        matchIndex = new LinkedList<>();
        for (int i = 0; i < numberOfReplicas; i++) {
            nextIndex.add(lastLogIndex + 1);
            matchIndex.add(0);
        }
        System.out.println("Next index of each replica: " +nextIndex);
    }
    private void incLastLogIndex() {
        lastLogIndex++;
    }
    public void updateCommitIndexLeader() {
        PriorityQueue<Integer> majority = new PriorityQueue<>((o1, o2) -> {
            if (Objects.equals(o1, o2)) return 0;
            return o1 > o2 ? -1 : +1;
        });
        majority.addAll(nextIndex);
        int aux = nextIndex.size() / 2 + 1;
        int commitIndexAux = 0;
        while (majority.peek() != null && --aux >= 0) {
            commitIndexAux = majority.poll() - 1; //-1 porque o matchIndex é o proximo a receber, logo sabemos que todos têm o mat
        }
        System.out.println(nextIndex);
        //update commitIndex
        commitIndex = commitIndexAux;
        System.out.println("New commit index: " + commitIndex);
        //atualizar maquina.
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
        while(log.size() >= listLogsCommitted) {
            log.remove();
        }
        lastLogIndex = FileManager.getNumberOfLines(logFile) + listLogsCommitted;
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
        ++listLogsCommitted;
        ++commitIndex;
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
        //System.out.println("Replica: "+ replicaId + ", nextIndex updated to: " + index + ".");
        nextIndex.set(replicaId, index);
    }
    public void incNextIndex(int replicaId) {
        //System.out.println("Replica: "+ replicaId + ", nextIndex updated to: " + nextIndex.get(replicaId) + 1 + ".");
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
        if(commitIndex == -1 || commitIndex <= lastApplied) return; //if it doesn't exist any entry to commit
        System.out.println("--> Updating state machine...");
        while(commitIndex > lastApplied) {
            int toApplyIndex = lastApplied + 1;
            LogElement.LogElementArgs element = log.get(toApplyIndex - numberOfFileLines);
            stateMachine.decodeLogElement(element);
            incLastApplied();
        }
        System.out.println("Current state of machine: " + stateMachine.getCounter());
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    //TODO: nao respeita os casos do ficheiro.
    public LogElement.LogElementArgs getEntry(int index) {
        System.out.println("Index: " + index);
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

    public LinkedList<LogElement.LogElementArgs> getEntries(int replicaId) {
        //index is the next index to send to the follower.
        int index = nextIndex.get(replicaId);
        System.out.println("ReplicaId: " + replicaId + ", index: " + index);
        LinkedList<LogElement.LogElementArgs> entries = new LinkedList<>();
        /*for (int i = index; i <= lastLogIndex; i++) {
            System.out.println(ByteBuffer.wrap(log.get(i).getCommandArgs()).getInt());
            entries.add(log.get(i));
        }
        return entries;*/

        //int linesToReadFromFile = (lastLogIndex - index) - log.size();
        //if (linesToReadFromFile == 0) return log;
        //if (linesToReadFromFile < 0) {
        if (log.size() > 0) {
            /*
            for (LogElement.LogElementArgs logElementArgs : log) {
                System.out.println(ByteBuffer.wrap(logElementArgs.getCommandArgs()).getInt());
            }*/
            System.out.println("Index replica: " + index);
            System.out.println("lastLogIndex replica: " + lastLogIndex);
            for (int i = index; i <= lastLogIndex; i++) {
                System.out.println("Value:");
                System.out.println(ByteBuffer.wrap(log.get(i).getCommandArgs()).getInt());
                entries.add(log.get(i));
            }

            for (int i = log.size() - (lastLogIndex - index); i < log.size(); i++) {
                System.out.println(ByteBuffer.wrap(log.get(i).getCommandArgs()).getInt());
                //entries.add(log.get(i));
            }
            for (LogElement.LogElementArgs entry: entries) {
                System.out.println("Entry: " + ByteBuffer.wrap(entry.getCommandArgs()).getInt());
            }
            return entries;
        }/*
        FileManager.readLastNLines(logFile, linesToReadFromFile)
                .forEach(line -> entries.add(LogElement.jsonToLogElement(line)));
        entries.addAll(log);*/
        return entries;
    }

    //se a diferença entre o index e o commitIndex for superior à lista de logs, temos que
    //ir buscar alguns ao ficheiro. Ou seja, se o index for 20, e o commitIndex for 35, quer dizer
    //que o lider tem que enviar 15 entries. Por exemplo, caso a lista apenas contenha 10 entries,
    //temos que ir ao ficheiro buscar as últimas 5 linhas, para podermos enviar as 15 que o follower necessita.

    //se a seguinte variavel, se possuir um valor superior a 0, é o número de linhas a ir buscar ao ficheiro
    //se o número for 0, basta retornar toda a lista log.
    //se o número for inferior a 0, quer dizer que as entries necessarias estao todas na log, é ir buscar as ultimas commitIndex-index
}
