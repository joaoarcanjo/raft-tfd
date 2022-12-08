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

    public void decodeLogElement(LogElement.LogElementArgs element) {
        switch (element.getLabel()) {
            case ("increaseBy"): {
                try {
                    int param = ByteBuffer.wrap(element.getCommandArgs()).getInt();
                    increaseBy(param);
                    System.out.println("==========================================");
                    System.out.println("LOG: New log commited!");
                    System.out.println(" ---> label: " + element.getLabel());
                    System.out.println(" ---> args: " + param);
                    System.out.println("LOG: My machine current state: "+ getCounter());
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