import java.util.HashSet;

public class EventHandler {

    public static boolean add(String message) {
        System.out.println("Add");
        return Replica.addMessage(message);
    }

    public static HashSet<String> get() {
        System.out.println("Get");
        return Replica.getReplicaMessages();
    }
}
