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

    }
}