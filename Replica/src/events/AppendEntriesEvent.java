package events;

import com.google.protobuf.Timestamp;
import replica.Result;

public class AppendEntriesEvent implements EventHandler {
    public static final String LABEL = "APPEND";

    @Override
    public Result processRequest(int senderId, String label, String data, Timestamp timestamp) {
        return null;
    }

    @Override
    public void processSelfRequest(String label, String data) {

    }
}
