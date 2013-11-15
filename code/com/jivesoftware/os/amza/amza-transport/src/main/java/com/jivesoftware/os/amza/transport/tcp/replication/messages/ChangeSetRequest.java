package com.jivesoftware.os.amza.transport.tcp.replication.messages;

import com.jivesoftware.os.amza.transport.tcp.replication.shared.FrameableMessage;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

/**
 *
 */
public class ChangeSetRequest implements FrameableMessage {

    @Override
    public void serialize(FSTObjectOutput output) throws IOException {
        //do nothing - empty body
    }
}
