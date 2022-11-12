package events.objects;

import com.google.gson.Gson;

public class RequestVoteRPC {
    public static class RequestVoteArgs {
        public int term;
        public int candidateId;
        public int lastLogIndex;
        public int lastLogTerm;

        public RequestVoteArgs(State state, int candidateId) {
            this.term = state.getCurrentTerm();
            this.candidateId = candidateId;
            this.lastLogIndex = state.getLastLogIndex();
            this.lastLogTerm = state.getLastLogTerm();
        }
    }

    /**
     * Receives a state and the candidateId and will return a string with json format with the arguments needed.
     * @param state
     * @param candidateId
     * @return
     */
    public static String requestVoteArgsToJson(State state, int candidateId) {
        return new Gson().toJson(new RequestVoteArgs(state, candidateId));
    }

    /**
     * Receives a string with json format with the arguments and will return an RequestVoteArgs object.
     * @param json
     * @return
     */
    public static RequestVoteArgs requestVoteArgsFromJson(String json) {
        return new Gson().fromJson(json, RequestVoteArgs.class);
    }
}



