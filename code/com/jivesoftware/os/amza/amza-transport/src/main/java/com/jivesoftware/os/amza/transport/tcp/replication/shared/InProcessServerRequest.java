package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.transport.tcp.replication.messages.FrameableMessage;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class InProcessServerRequest {

    private final MessageFramer messageFramer;
    private final BufferProvider bufferProvider;
    private final ByteBuffer readBuffer;
    private final AtomicReference<FrameableMessage> message;

    public InProcessServerRequest(MessageFramer messageFramer, BufferProvider bufferProvider) {
        this.messageFramer = messageFramer;
        this.bufferProvider = bufferProvider;
        this.readBuffer = bufferProvider.acquire();
        this.message = new AtomicReference<>();
    }

    public boolean readRequest(SocketChannel channel) throws Exception {
        try {
            int read = channel.read(readBuffer);
            if (read > 0) {
                FrameableMessage request = messageFramer.fromFrame(readBuffer, FrameableMessage.class); //???
                if (request != null) {
                    message.set(request);
                    bufferProvider.release(readBuffer);
                }
                return true;
            } else {
                bufferProvider.release(readBuffer);
                return false;
            }
        } catch (Exception ex) {
            bufferProvider.release(readBuffer);
            throw ex;
        }
    }

    public FrameableMessage getRequest() {
        return message.get();
    }
}
