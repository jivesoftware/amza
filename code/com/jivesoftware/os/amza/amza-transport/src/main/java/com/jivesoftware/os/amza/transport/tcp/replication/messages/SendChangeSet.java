package com.jivesoftware.os.amza.transport.tcp.replication.messages;

import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.FrameableMessage;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;
import java.util.NavigableMap;

/**
 *
 */
public class SendChangeSet implements FrameableMessage {

    private final TableName mapName;
    private final NavigableMap changes;

    public SendChangeSet(TableName mapName, NavigableMap changes) {
        this.mapName = mapName;
        this.changes = changes;
    }

    @Override
    public void serialize(FSTObjectOutput output) throws IOException {
        output.writeObject(mapName, TableName.class);
        output.writeObject(changes, NavigableMap.class); //todo - what about the elements?
    }

    public TableName getMapName() {
        return mapName;
    }

    public NavigableMap getChanges() {
        return changes;
    }
}
