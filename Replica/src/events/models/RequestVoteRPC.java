package events.models;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class RequestVoteRPC {
    public static class RequestVoteArgs {
        public int term;
        public int candidateId;
        public int lastLogIndex;
        public int lastLogTerm;

        public RequestVoteArgs(State state, int candidateId) {
            this.term = state.getCurrentTerm();
            this.candidateId = candidateId;
            this.lastLogIndex = state.getCommitIndex();
            this.lastLogTerm = state.getLastLogTerm();
        }
    }

    public static class ResultVote {

        public int term;
        public boolean vote;
        public ResultVote(int term, boolean vote) {
            this.term = term;
            this.vote = vote;
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

    /**
     * Receives a string with json format with the arguments and will return an ResultVote object.
     * @param json
     * @return
     */
    public static ResultVote resultVoteFromJson(String json) {
        return new Gson().fromJson(json, ResultVote.class);
    }

    /**
     * Receives a term and the vote. Returns a string with json format with the arguments needed.
     * @param term
     * @param vote
     * @return
     */
    public static String resultVoteToJson(int term, boolean vote) {
        return new Gson().toJson(new ResultVote(term, vote));
    }

    public static boolean verifyResultVoteSyntax(String json) {
        try {
            new Gson().fromJson(json, ResultVote.class);
            return true;
        } catch (JsonSyntaxException e) {
            return false;
        }
    }
}



