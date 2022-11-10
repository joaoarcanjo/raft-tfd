package events;

import com.google.protobuf.Timestamp;
import replica.Result;

public interface EventHandler {
    Result processRequest(int senderId, String label, String data, Timestamp timestamp);
    void processSelfRequest(String label, String data);
}
