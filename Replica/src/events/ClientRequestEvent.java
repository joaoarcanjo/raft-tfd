package events;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import common.Replica;
import events.models.LogElement;
import events.models.State;
import replica.Result;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ClientRequestEvent implements EventHandler {

    public static final String LABEL = "CLIENT";
    public static final HashSet<String> OPERATIONS = new HashSet<>(List.of("increaseBy"));

    private final Condition condition;
    private final ReentrantLock monitor;
    private final State state;


    public ClientRequestEvent(ReentrantLock monitor, Condition condition, State state) {
        this.condition = condition;
        this.monitor = monitor;
        this.state = state;
    }

    @Override
    public Result processRequest(int senderId, String label, ByteString data, Timestamp timestamp) {
        return Result.newBuilder().build(); // TODO:
    }

    @Override
    public void processSelfRequest(String label, String data) {
        // Can't reach here
    }
}
