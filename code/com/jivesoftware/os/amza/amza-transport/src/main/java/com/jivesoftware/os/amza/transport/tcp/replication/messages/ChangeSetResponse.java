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
    private long interactionId;

    /**
     * for serialization
     */
    public ChangeSetResponse() {
    }

    public ChangeSetResponse(TransactionSet transactionSet, boolean lastInSequence, long interactionId) {
        this.transactionSet = transactionSet;
        this.lastInSequence = lastInSequence;
        this.interactionId = interactionId;
    }

    public TransactionSet getTransactionSet() {
        return transactionSet;
    }

    @Override
    public long getInteractionId() {
        return interactionId;
    }

    @Override
    public boolean isLastInSequence() {
        return lastInSequence;
    }

    @Override
    public void serialize(FSTObjectOutput output) throws IOException {
        output.writeBoolean(lastInSequence);
        output.writeFLong(interactionId);
        output.writeObject(transactionSet); //TODO what about sub objects?
    }

    @Override
    public void deserialize(FSTObjectInput input) throws Exception {
        this.lastInSequence = input.readBoolean();
        this.interactionId = input.readLong();
        this.transactionSet = (TransactionSet) input.readObject(TransactionSet.class);
    }
}
