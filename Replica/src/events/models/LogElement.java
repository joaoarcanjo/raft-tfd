package events.models;

import com.google.gson.Gson;

public class LogElement {
    public static class LogElementArgs {
        private final byte[] commandArgs;
        private final String label;
        private final int term;

        public LogElementArgs(byte[] commandArgs, String label, int term) {
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

    public static String logElementToJson(LogElementArgs logElement) {
        return new Gson().toJson(logElement);
    }

    public static LogElementArgs jsonToLogElement(String json) {
        return new Gson().fromJson(json, LogElementArgs.class);
    }
}
