package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.transport.tcp.replication.messages.FrameableMessage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 *
 */
public class InProcessServerResponse {

    private final BufferProvider bufferProvider;
    private final ByteBuffer writeBuffer;
    private final long interactionId;
    private final boolean lastInSequence;

    public InProcessServerResponse(MessageFramer messageFramer, BufferProvider bufferProvider, FrameableMessage response) throws IOException {
        this.bufferProvider = bufferProvider;
        this.writeBuffer = bufferProvider.acquire();
        this.interactionId = response.getInteractionId();
        this.lastInSequence = response.isLastInSequence();

        messageFramer.toFrame(response, writeBuffer);
    }

    public long getInteractionId() {
        return interactionId;
    }

    public boolean isLastInSequence() {
        return lastInSequence;
    }

    boolean writeResponse(SocketChannel channel) throws IOException {
        try {
            channel.write(writeBuffer);
            if (writeBuffer.remaining() == 0) {
                bufferProvider.release(writeBuffer);
                return true;
            } else {
                return false;
            }
        } catch (Exception ex) {
            bufferProvider.release(writeBuffer);
            throw ex;
        }
    }
}
