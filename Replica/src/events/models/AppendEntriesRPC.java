package events.models;

import com.google.gson.Gson;

import java.util.LinkedList;

public class AppendEntriesRPC {

    public static class AppendEntriesArgs {
        public int term;
        public int leaderId;
        public LinkedList<LogElement.LogElementArgs> entries; // Can be changed to T so that becomes generic
        public int prevLogIndex;
        public int prevLogTerm;
        public int leaderCommit;

        public AppendEntriesArgs(State state, LinkedList<LogElement.LogElementArgs> entries) {
            this.term = state.getCurrentTerm();
            this.leaderId = state.getCurrentLeader();
            this.entries = entries;
            this.prevLogIndex = state.getCommitIndex();
            this.prevLogTerm = state.getLastLogTerm();
            this.leaderCommit = state.getLastApplied();
        }
    }

    public static class ResultAppendEntry {
        public int term;
        public int nextIndex;
        public ResultAppendEntry(int term, int nextIndex) {
            this.term = term;
            this.nextIndex = nextIndex;
        }
    }

    /**
     * Receives a state and the candidateId and will return a string with json format with the arguments needed.
     *
     * @param state
     * @return
     */
    public static String appendEntriesArgsToJson(State state, LinkedList<LogElement.LogElementArgs> entries) {
        return new Gson().toJson(new AppendEntriesRPC.AppendEntriesArgs(state, entries));
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

    /**
     * Receives a string with json format with the arguments and will return an ResultAppendEntry object.
     * @param json
     * @return
     */
    public static ResultAppendEntry resultAppendEntryFromJson(String json) {
        return new Gson().fromJson(json, ResultAppendEntry.class);
    }

    /**
     * Receives a term and the nextIndex. Returns a string with json format with the arguments needed.
     * @param term
     * @param nextIndex
     * @return
     */
    public static String resultAppendEntryToJson(int term, int nextIndex) {
        return new Gson().toJson(new ResultAppendEntry(term, nextIndex));
    }

}