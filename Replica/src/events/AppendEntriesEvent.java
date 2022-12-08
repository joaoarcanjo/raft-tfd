package events;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import events.models.AppendEntriesRPC;
import events.models.State;
import replica.Result;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class AppendEntriesEvent implements EventHandler {
    public static final String LABEL = "APPEND";
    private final Condition condition;
    private final ReentrantLock monitor;
    private final State state;

    public AppendEntriesEvent(ReentrantLock monitor, Condition condition, State state) {
        this.condition = condition;
        this.monitor = monitor;
        this.state = state;
    }

    @Override
    public Result processRequest(int senderId, String label, ByteString data, Timestamp timestamp) {
        monitor.lock();
        try {
            AppendEntriesRPC.AppendEntriesArgs received = AppendEntriesRPC.appendEntriesArgsFromJson(data.toStringUtf8());

            if (received.term >= state.getCurrentTerm()) {
                state.setCurrentTerm(received.term);

                //If it is a heartbeat.
                if (received.entries.isEmpty()) {
                    if (received.leaderId != state.getCurrentLeader()) {
                        state.setCurrentLeader(received.leaderId);
                    }
                    System.out.println("* Heartbeat received from " + received.leaderId + " *");
                    condition.signal();
                } else {

                    //fazer commit do prevLogIndex? Se o prevLogIndex for superior ao commitIndex
                    //eu quero todas as entries desde o prevLogIndex + 1 até à ultima entry do lider.
                    System.out.println("LOG: prevLogIndex: "+ received.prevLogIndex);
                    System.out.println("LOG: commitIndex: "+ state.getCommitIndex());
                    if (received.prevLogIndex > state.getCommitIndex()) {
                        state.deleteUncommittedLogs();
                    }
                    if (received.prevLogIndex == state.getCommitIndex()) {
                        state.updateStateMachine();
                        state.incCommitIndex();
                        System.out.println("Received entries: " + received.entries);
                        state.addToLog(received.entries.get(0));
                    }

                    //Will return to the leader, the last commit index.
                    return Result.newBuilder()
                            .setId(senderId)
                            .setResultMessage(
                                    AppendEntriesRPC.resultAppendEntryToJson(
                                            state.getCurrentTerm(),
                                            state.getCommitIndex()
                                    ))
                            .build();
                }
                // Notify the condition because receives a heartbeat
                // Notify leader so that the while breaks, also, if its follower, reset loop
            }
            return null;
        } finally {
            monitor.unlock();
        }
    }

    @Override
    public void processSelfRequest(String label, String data) {

    }
}
