package events;

import com.google.protobuf.Timestamp;
import replica.Result;

import java.util.concurrent.locks.Condition;

public class AppendEntriesEvent implements EventHandler {
    public static final String LABEL = "APPEND";
    private final Condition condition;

    public AppendEntriesEvent(Condition condition) {
        this.condition = condition;
    }

    @Override
    public Result processRequest(int senderId, String label, String data, Timestamp timestamp) {
        if (data.isEmpty()) {
            condition.notify();
        }
        return null;
    }

    @Override
    public void processSelfRequest(String label, String data) {

    }
}