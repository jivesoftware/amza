package com.jivesoftware.os.amza.transport.tcp.replication.protocol;

import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

/**
 *
 */
public class ChangeSetRequestPayload implements MessagePayload {

    private TableName mapName;
    private long highestTransactionId;

    public ChangeSetRequestPayload() {
    }

    public ChangeSetRequestPayload(TableName mapName, long highestTransactionId) {
        this.mapName = mapName;
        this.highestTransactionId = highestTransactionId;
    }

    @Override
    public void serialize(FSTObjectOutput output) throws IOException {
        output.writeObject(mapName);
        output.writeLong(highestTransactionId);
    }

    @Override
    public void deserialize(FSTObjectInput input) throws Exception {
        this.mapName = (TableName) input.readObject();
        this.highestTransactionId = input.readLong();
    }

    public TableName getMapName() {
        return mapName;
    }

    public long getHighestTransactionId() {
        return highestTransactionId;
    }
}
