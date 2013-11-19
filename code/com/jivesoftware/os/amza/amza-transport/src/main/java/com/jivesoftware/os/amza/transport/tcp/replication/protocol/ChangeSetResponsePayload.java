package com.jivesoftware.os.amza.transport.tcp.replication.protocol;

import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

/**
 *
 */
public class ChangeSetResponsePayload implements MessagePayload {

    private TransactionSet transactionSet;

    /**
     * for serialization
     */
    public ChangeSetResponsePayload() {
    }

    public ChangeSetResponsePayload(TransactionSet transactionSet) {
        this.transactionSet = transactionSet;
    }

    public TransactionSet getTransactionSet() {
        return transactionSet;
    }

    @Override
    public void serialize(FSTObjectOutput output) throws IOException {
        output.writeObject(transactionSet);
    }

    @Override
    public void deserialize(FSTObjectInput input) throws Exception {
        this.transactionSet = (TransactionSet) input.readObject(TransactionSet.class);
    }
}
