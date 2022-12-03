package events;

import com.google.protobuf.ByteString;
import events.models.State;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class EventLogic {
    private final Set<String> receivedData;
    private final Map<String, EventHandler> eventHandlers;

    public EventLogic(ReentrantLock monitor, Condition condition, State state) {
        receivedData = new HashSet<>();
        eventHandlers = new HashMap<>();
        registerHandler(AddEvent.LABEL, new AddEvent(this));
        registerHandler(GetEvent.LABEL, new GetEvent(this));
        registerHandler(AppendEntriesEvent.LABEL, new AppendEntriesEvent(monitor, condition, state));
        registerHandler(RequestVoteEvent.LABEL, new RequestVoteEvent(monitor, condition,state));
    }

    private void registerHandler(String requestLabel, EventHandler handler) {
        eventHandlers.put(requestLabel, handler);
    }

    public Optional<EventHandler> getEventHandler(String requestLabel) {
        EventHandler handler = eventHandlers.get(requestLabel);
        return handler == null ? Optional.empty() : Optional.of(handler);
    }

    public Set<String> getReceivedData() {
        return receivedData;
    }

    public boolean addData(ByteString data) {
        return receivedData.add(String.valueOf(data));
    }

    public static void verifyLabel(String requestLabel, String handlerLabel) {
        if (requestLabel.compareTo(handlerLabel) != 0)
            throw new IllegalArgumentException("The label provided doesn't match with the " + handlerLabel + " label.");
    }
}
