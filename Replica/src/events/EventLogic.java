package events;

import com.google.protobuf.Timestamp;
import replica.Result;
import replica.ResultList;

import java.util.*;

public class EventLogic {
    public enum Events { ADD, GET }

    private final Set<String> receivedData;
    private final Map<String, EventHandler> eventHandlers;

    public EventLogic() {
        receivedData = new HashSet<>();
        eventHandlers = new HashMap<>();
        registerHandler(Events.ADD.name(), new ProcessAddRequest());
        registerHandler(Events.GET.name(), new ProcessGetRequest());
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

    private static void verifyLabel(String requestLabel, String handlerLabel) {
        if (requestLabel.compareTo(handlerLabel) != 0)
            throw new IllegalArgumentException("The label provided doesn't match with the " + handlerLabel + " label.");
    }

    private class ProcessAddRequest implements EventHandler {
        public final String LABEL = Events.ADD.name();

        @Override
        public Result processRequest(int senderId, String label, String data, Timestamp timestamp) {
            verifyLabel(label, LABEL);
            String resultMessage;
            if (receivedData.add(data))
                resultMessage = "Requested data added with success.";
            else
                resultMessage = "The requested data is already in the set.";
            return Result.newBuilder().setResultMessage(resultMessage).setId(senderId).setTimestamp(timestamp).build();
        }
    }

    private class ProcessGetRequest implements EventHandler {
        public final String LABEL = Events.GET.name();

        @Override
        public Result processRequest(int senderId, String label, String data, Timestamp timestamp) {
            verifyLabel(label, LABEL);
            return Result.newBuilder()
                    .setResults(ResultList.newBuilder().addAllList(receivedData).build())
                    .setId(senderId)
                    .setTimestamp(timestamp)
                    .build();
        }
    }
}
