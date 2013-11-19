package com.jivesoftware.os.amza.transport.tcp.replication.protocol;

import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;
import java.util.NavigableMap;

/**
 *
 */
public class SendChangeSetPayload implements MessagePayload {

    private TableName mapName;
    private NavigableMap changes;

    /**
     * for serialization
     */
    public SendChangeSetPayload() {
    }

    public SendChangeSetPayload(TableName mapName, NavigableMap changes) {
        this.mapName = mapName;
        this.changes = changes;
    }

    @Override
    public void serialize(FSTObjectOutput output) throws IOException {
        output.writeObject(mapName, TableName.class);
        output.writeObject(changes, NavigableMap.class);
    }

    @Override
    public void deserialize(FSTObjectInput input) throws Exception {
        this.mapName = (TableName) input.readObject(TableName.class);
        this.changes = (NavigableMap) input.readObject(NavigableMap.class);
    }

    public TableName getMapName() {
        return mapName;
    }

    public NavigableMap getChanges() {
        return changes;
    }

    @Override
    public String toString() {
        return "SendChangeSetPayload{" + "mapName=" + mapName + ", changes=" + changes + '}';
    }
}
