package com.jivesoftware.os.amza.transport.tcp.replication.messages;

import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.FrameableMessage;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

/**
 *
 */
public class ChangeSetResponse implements FrameableMessage {

    private TransactionSet transactionSet;
    private boolean lastInSequence;

    public ChangeSetResponse(TransactionSet transactionSet, boolean lastInSequence) {
        this.transactionSet = transactionSet;
        this.lastInSequence = lastInSequence;
    }

    public TransactionSet getTransactionSet() {
        return transactionSet;
    }

    @Override
    public void serialize(FSTObjectOutput output) throws IOException {
        output.writeBoolean(lastInSequence);
        output.writeObject(transactionSet); //TODO what about sub objects?
    }

    public boolean isLastInSequence() {
        return lastInSequence;
    }
}
