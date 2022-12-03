package events;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import replica.Result;

public class AddEvent implements EventHandler {
    public static final String LABEL = "ADD";
    private final EventLogic eventLogic;

    public AddEvent(EventLogic eventLogic) {
        this.eventLogic = eventLogic;
    }

    @Override
    public Result processRequest(int senderId, String label, ByteString data, Timestamp timestamp) {
        EventLogic.verifyLabel(label, LABEL);
        String resultMessage;
        if (eventLogic.addData(data))
            resultMessage = "Requested data added with success.";
        else
            resultMessage = "The requested data is already in the set.";
        return Result.newBuilder().setResultMessage(resultMessage).setId(senderId).setTimestamp(timestamp).build();
    }

    @Override
    public void processSelfRequest(String label, String data) {
        EventLogic.verifyLabel(label, LABEL);
        if (eventLogic.addData(ByteString.copyFromUtf8(data)))
            System.out.println("Requested data added with success.");
        else
            System.out.println("The requested data is already in the set.");
    }
}
