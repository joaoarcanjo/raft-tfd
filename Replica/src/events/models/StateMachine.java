package events.models;

import java.nio.ByteBuffer;

public class StateMachine {
    private int counter = 0;

    public int getCounter() {
        return counter;
    }
    public void increaseBy(int x) {
        counter += x;
    }

    public void decodeLogElement(LogElement logElement) {
        switch (logElement.getLabel()) {
            case ("increaseBy"): {
                try {
                    increaseBy(ByteBuffer.wrap(logElement.getCommandArgs()).getInt());
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