package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 */
public class BufferProvider {

    private final Queue<ByteBuffer> buffers;

    public BufferProvider(int bufferSize, int poolSize) {
        this.buffers = new ConcurrentLinkedQueue<>();
        for (int i=0; i<poolSize; i++) {
            buffers.add(ByteBuffer.allocateDirect(bufferSize));
        }
    }

    public ByteBuffer acquire() {
        return buffers.remove();
    }

    public void release(ByteBuffer byteBuffer) {
        byteBuffer.clear();
        buffers.add(byteBuffer);
    }
}
