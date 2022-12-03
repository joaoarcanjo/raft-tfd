package events.models;

public class LogElement {
    private final byte[] commandArgs;
    private final String label;
    private final int term;

    public LogElement(byte[] commandArgs, String label, int term) {
        this.commandArgs = commandArgs;
        this.label = label;
        this.term = term;
    }

    public byte[] getCommandArgs() {
        return commandArgs;
    }

    public String getLabel() {
        return label;
    }
    public int getTerm() {
        return term;
    }
}
