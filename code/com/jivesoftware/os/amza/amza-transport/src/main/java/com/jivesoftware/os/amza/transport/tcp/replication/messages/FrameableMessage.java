package com.jivesoftware.os.amza.transport.tcp.replication.messages;

import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * All tcp replication messages implement this interface. Implementors must have a public no-args constructor for serialization to work. Janky, but hopefully
 * temporary.
 */
public interface FrameableMessage extends Serializable {

    void serialize(FSTObjectOutput output) throws IOException;

    void deserialize(FSTObjectInput input) throws Exception;

    long getInteractionId();

    boolean isLastInSequence();
}
