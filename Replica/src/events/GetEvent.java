package events;

import com.google.protobuf.Timestamp;
import replica.Result;
import replica.ResultList;

public class GetEvent implements EventHandler {
    public static final String LABEL = "GET";
    private final EventLogic eventLogic;

    public GetEvent(EventLogic eventLogic) {
        this.eventLogic = eventLogic;
    }

    @Override
    public Result processRequest(int senderId, String label, String data, Timestamp timestamp) {
        EventLogic.verifyLabel(label, LABEL);
        return Result.newBuilder()
                .setResults(ResultList.newBuilder().addAllList(eventLogic.getReceivedData()).build())
                .setId(senderId)
                .setTimestamp(timestamp)
                .build();
    }

    @Override
    public void processSelfRequest(String label, String data) {
        EventLogic.verifyLabel(label, LABEL);
        System.out.println("ResultList: " + eventLogic.getReceivedData());
    }
}
