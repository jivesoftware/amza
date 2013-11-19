package com.jivesoftware.os.amza.transport.tcp.replication.messages;

import com.jivesoftware.os.amza.shared.TransactionSet;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

/**
 *
 */
public class ChangeSetResponse implements FrameableMessage {

    private TransactionSet transactionSet;
    private boolean lastInSequence;

    /**
     * for serialization
     */
    public ChangeSetResponse() {
    }

    public ChangeSetResponse(TransactionSet transactionSet) {
        this.transactionSet = transactionSet;
    }

    public TransactionSet getTransactionSet() {
        return transactionSet;
    }

    @Override
    public void serialize(FSTObjectOutput output) throws IOException {
        output.writeObject(transactionSet); //TODO what about sub objects?
    }

    @Override
    public void deserialize(FSTObjectInput input) throws Exception {
        this.transactionSet = (TransactionSet) input.readObject(TransactionSet.class);
    }
}
