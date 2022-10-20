import replica.Result;
import replica.ResultList;

import java.util.HashSet;

public class ServletLogic {

    public Result processRequest(String senderID, String requestLabel, String requestData) {

        System.out.println("Processing request from: " + senderID);
        switch (requestLabel) {
            case "ADD": {
                return Result.newBuilder().setAck(EventHandler.add(requestData)).build();
            }
            case "GET": {
                HashSet<String> messages = EventHandler.get();
                return Result.newBuilder().setResults(ResultList.newBuilder().addAllList(messages).build()).build();
            }
            default: return null;
        }
    }
}
