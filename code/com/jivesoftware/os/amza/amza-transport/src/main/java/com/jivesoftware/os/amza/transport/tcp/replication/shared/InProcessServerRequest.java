package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.transport.tcp.replication.messages.FrameableMessage;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class InProcessServerRequest implements Comparable<InProcessServerRequest> {

    private final MetricLogger LOG = MetricLoggerFactory.getLogger();
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

            LOG.trace("Read {} bytes from connection to {}", read, channel.getRemoteAddress());

            if (read > 0) {
                //TODO this does not work - using the FrameableMessage interface rather than the concreate
                //class blows up with ClassNotFound. Need to work some fst magic or start sending opcodes to use
                //as a message class lookup key
                FrameableMessage request = messageFramer.fromFrame(readBuffer, FrameableMessage.class);
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

    public void releaseResources() {
        bufferProvider.release(readBuffer);
    }

    @Override
    public int compareTo(InProcessServerRequest o) {
        return Integer.compare(System.identityHashCode(this), System.identityHashCode(o));
    }
}
