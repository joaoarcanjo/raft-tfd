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
        Result result = null;
        monitor.lock();
        try {
            AppendEntriesRPC.AppendEntriesArgs received = AppendEntriesRPC.appendEntriesArgsFromJson(data.toStringUtf8());
            //System.out.println("Received term: " + received.term);
            //System.out.println("State current term: " + state.getCurrentTerm());
            if (received.term >= state.getCurrentTerm()) {
                state.setCurrentTerm(received.term);
                //If it is a heartbeat.
                if (received.entries.isEmpty()) {
                    if (received.leaderId != state.getCurrentLeader()) {
                        state.setCurrentLeader(received.leaderId);
                    }
                    state.setCommitIndex(received.leaderCommit);
                    state.updateStateMachine();

                    System.out.println("* Heartbeat received from " + received.leaderId + " *");
                    condition.signal();
                } else {
                    System.out.println("\n## New entries received from " + received.leaderId + " ##");
                    //System.out.println("PrevLogIndex: " + received.prevLogIndex);
                    //System.out.println("LastLogIndex: " + state.getLastLogIndex());

                    //System.out.println("PrevLogTerm: " + received.prevLogTerm);
                    //System.out.println("LastLogTerm: " + state.getLastLogTerm());

                    if (received.prevLogIndex == state.getLastLogIndex() && (received.prevLogTerm == state.getLastLogTerm())) {
                        System.out.println("--> Number of entries received: " + received.entries.size() + "<--");
                        //System.out.println("Last log index: " + state.getLastLogIndex());
                        received.entries.forEach(state::addToLog);
                        //System.out.println("Leader commit index received: " + received.leaderCommit);
                        state.setCommitIndex(received.leaderCommit);
                        state.updateStateMachine();
                    } else {
                        state.deleteUncommittedLogs();
                    }

                    int nextIndex = state.getLastLogIndex() + 1;
                    System.out.println("--> My next index is: " + nextIndex + " <--");

                    result = Result.newBuilder()
                            .setId(senderId)
                            .setResultMessage(
                                    AppendEntriesRPC.resultAppendEntryToJson(
                                            state.getCurrentTerm(),
                                            nextIndex
                                    ))
                            .setLabel("entriesResponse")
                            .build();
                    condition.signal();
                }
            }
            return result;
        } finally {
            monitor.unlock();
        }
    }

    @Override
    public void processSelfRequest(String label, String data) {

    }
}
