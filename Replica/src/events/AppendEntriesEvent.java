package events;

import com.google.protobuf.Timestamp;
import events.objects.AppendEntriesRPC;
import events.objects.State;
import replica.Result;

import java.util.concurrent.locks.Condition;

public class AppendEntriesEvent implements EventHandler {
    public static final String LABEL = "APPEND";
    private final Condition condition;

    private final State state;

    public AppendEntriesEvent(Condition condition, State state) {
        this.condition = condition;
        this.state = state;
    }

    @Override
    public Result processRequest(int senderId, String label, String data, Timestamp timestamp) {

        AppendEntriesRPC.AppendEntriesArgs received = AppendEntriesRPC.appendEntriesArgsFromJson(data);

        if (received.entries.isEmpty() && received.term > state.getCurrentTerm()) {
            condition.notify(); // Notify leader so that the while breaks, also, if its follower, reset loop
        }
        return null;
    }

    @Override
    public void processSelfRequest(String label, String data) {

    }
}
