package events;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import events.models.AppendEntriesRPC;
import events.models.State;
import replica.Result;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class AppendEntriesEvent implements EventHandler {
    public static final String LABEL = "APPEND";
    private final Condition condition;
    private final ReentrantLock monitor;
    private final State state;

    public AppendEntriesEvent(ReentrantLock monitor, Condition condition, State state) {
        this.condition = condition;
        this.monitor = monitor;
        this.state = state;
    }

    //prevLogIndex é a entry antes das novas, ou seja, prevLogIndex + 1 tem q ser igual ao nextIndex do follower.


    /*:Quando recebo entries, a primeira tem q possuir um index, relativamente à ultima q tenho, se não tiver
    tenho q avisar para na proxima me enviar a partir do meu index + 1. Se a propriedade anterior for válida,
    tenho q verificar também se o term é superior ou igual ao meu ultimo log entry. DONE I THINK */


    //Quando recebo o leader commited, sei qual é o index que posso realizar commit, posso atualizar o commitIndex,
    //que é qual é a entry máxima que sei q posso realizar commit, quando recebo entries, se a minha ultima entry
    //for superior ou igual ao commitIndex, eu vou atualizar a minha state machine até à entry que corresponde ao commitIndex
    //e atualio o lastApplied. Se o lastApplied for igual ao commitIndex, n faço nada.

    //Se eu recebo uma entry, cujo term é superior ao term do meu ultimo log, eu vou apagar os meus uncommitted entries,
    //e peço ao lider todos os logs a partir do ultimo que fiz commit, e ele envia me todos e assim fico consistente.

    //Se o index que o client necessita for inferior ao ultimo que eu enviei, faço invoke logo com todos os indexs que
    //lhe faltam, assim os logs vão estar consistentes.


    @Override
    public Result processRequest(int senderId, String label, ByteString data, Timestamp timestamp) {
        Result result = null;
        monitor.lock();
        try {
            AppendEntriesRPC.AppendEntriesArgs received = AppendEntriesRPC.appendEntriesArgsFromJson(data.toStringUtf8());

            if (received.term >= state.getCurrentTerm()) {
                state.setCurrentTerm(received.term);
                //If it is a heartbeat.
                if (received.entries.isEmpty()) {
                    if (received.leaderId != state.getCurrentLeader()) {
                        state.setCurrentLeader(received.leaderId);
                    }
                    System.out.println("* Heartbeat received from " + received.leaderId + " *");
                    condition.signal();
                } else {

                    //System.out.println("PrevLogIndex: " + received.prevLogIndex);
                    //System.out.println("LastLogIndex: " + state.getLastLogIndex());

                    if (received.prevLogIndex == state.getLastLogIndex()) {

                        System.out.println("PrevLogTerm: " + received.prevLogTerm);
                        System.out.println("LastLogTerm: " + state.getLastLogTerm());

                        if((received.prevLogTerm >= state.getLastLogTerm())) {
                            System.out.println("Number of entries received: " + received.entries.size());
                            received.entries.forEach(state::addToLog);
                            //System.out.println("Leader commit index received: " + received.leaderCommit);
                            state.setCommitIndex(received.leaderCommit);
                            state.updateStateMachine();
                        }
                    }
                    int nextIndex = state.getLastLogIndex() + 1;
                    System.out.println("My next index is: " + nextIndex);

                    result = Result.newBuilder()
                            .setId(senderId)
                            .setResultMessage(
                                    AppendEntriesRPC.resultAppendEntryToJson(
                                            state.getCurrentTerm(),
                                            nextIndex
                                    ))
                            .setLabel("entriesResponse")
                            .build();
                    condition.signal();
                }
                // Notify the condition because receives a heartbeat
                // Notify leader so that the while breaks, also, if its follower, reset loop
            }
            return result;
        } finally {
            monitor.unlock();
        }
    }

    @Override
    public void processSelfRequest(String label, String data) {

    }
}
