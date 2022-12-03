package events.models;

import com.google.gson.Gson;

import java.util.LinkedList;

public class AppendEntriesRPC {

    public static class AppendEntriesArgs {
        public int term;
        public int leaderId;

        public LinkedList<String> entries; // Can be changed to T so that becomes generic

        //  public int prevLogIndex;

        // public int prevLogTerm;

        public AppendEntriesArgs(State state, int leaderId, LinkedList<String> entries) {
            this.term = state.getCurrentTerm();
            this.leaderId = leaderId;
            this.entries = entries;
            // this.prevLogIndex = state.getLastLogIndex();
            // this.prevLogTerm = state.getLastLogTerm();
        }
    }

    /**
     * Receives a state and the candidateId and will return a string with json format with the arguments needed.
     *
     * @param state
     * @param leaderId
     * @return
     */
    public static String appendEntriesArgsToJson(State state, int leaderId, LinkedList<String> entries) {
        return new Gson().toJson(new AppendEntriesRPC.AppendEntriesArgs(state, leaderId, entries));
    }

    /**
     * Receives a string with json format with the arguments and will return an AppendEntriesArgs object.
     *
     * @param json
     * @return
     */
    public static AppendEntriesRPC.AppendEntriesArgs appendEntriesArgsFromJson(String json) {
        return new Gson().fromJson(json, AppendEntriesRPC.AppendEntriesArgs.class);
    }
}