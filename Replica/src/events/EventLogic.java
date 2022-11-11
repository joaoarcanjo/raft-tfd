package events;

import java.util.*;
import java.util.concurrent.locks.Condition;

public class EventLogic {
    private final Set<String> receivedData;
    private final Map<String, EventHandler> eventHandlers;

    public EventLogic(Condition condition) {
        receivedData = new HashSet<>();
        eventHandlers = new HashMap<>();
        registerHandler(AddEvent.LABEL, new AddEvent(this));
        registerHandler(GetEvent.LABEL, new GetEvent(this));
        registerHandler(AppendEntriesEvent.LABEL, new AppendEntriesEvent(condition));
        registerHandler(RequestVoteEvent.LABEL, new RequestVoteEvent());
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

    public boolean addData(String data) {
        return receivedData.add(data);
    }

    public static void verifyLabel(String requestLabel, String handlerLabel) {
        if (requestLabel.compareTo(handlerLabel) != 0)
            throw new IllegalArgumentException("The label provided doesn't match with the " + handlerLabel + " label.");
    }
}
