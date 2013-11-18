package com.jivesoftware.os.amza.transport.tcp.replication.messages;

import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

/**
 *
 */
public class ChangeSetRequest implements FrameableMessage {

    private long interactionId;

    public ChangeSetRequest() {
    }

    public ChangeSetRequest(long interactionId) {
        this.interactionId = interactionId;
    }

    @Override
    public void serialize(FSTObjectOutput output) throws IOException {
    }

    @Override
    public void deserialize(FSTObjectInput input) throws Exception {
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
