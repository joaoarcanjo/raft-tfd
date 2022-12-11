package events.models;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;

public class StateMachine {
    public static final HashSet<String> OPERATIONS = new HashSet<>(List.of("increaseBy"));
    private int counter = 0;

    public int getCounter() {
        return counter;
    }
    public void increaseBy(int x) {
        counter += x;
    }

    public void decodeLogElement(LogElement.LogElementArgs element) {
        switch (element.getLabel()) {
            case ("increaseBy"): {
                try {
                    int param = ByteBuffer.wrap(element.getCommandArgs()).getInt();
                    increaseBy(param);
                } catch (Exception e) {
                    System.out.println("-> Command arg must be an integer.");
                }
                break;
            }
            default:
                System.out.println("-> Unrecognizable label.");
        }
    }
}