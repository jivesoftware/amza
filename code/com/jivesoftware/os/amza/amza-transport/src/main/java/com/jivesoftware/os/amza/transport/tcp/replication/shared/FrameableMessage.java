package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 *
 */
public interface FrameableMessage extends Serializable {

    void serialize(FSTObjectOutput output) throws IOException;
}
