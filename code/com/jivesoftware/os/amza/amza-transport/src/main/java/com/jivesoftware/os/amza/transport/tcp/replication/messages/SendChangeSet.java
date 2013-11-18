package com.jivesoftware.os.amza.transport.tcp.replication.messages;

import com.jivesoftware.os.amza.shared.TableName;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;
import java.util.NavigableMap;

/**
 *
 */
public class SendChangeSet implements FrameableMessage {

    private TableName mapName;
    private NavigableMap changes;
    private long interactionId;

    /**
     * for serialization
     */
    public SendChangeSet() {
    }

    public SendChangeSet(TableName mapName, NavigableMap changes, long interactionId) {
        this.mapName = mapName;
        this.changes = changes;
        this.interactionId = interactionId;
    }

    @Override
    public void serialize(FSTObjectOutput output) throws IOException {
        output.writeObject(mapName, TableName.class);
        output.writeObject(changes, NavigableMap.class); //todo - what about the elements?
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
    public long getInteractionId() {
        return interactionId;
    }

    @Override
    public boolean isLastInSequence() {
        return true;
    }
}
