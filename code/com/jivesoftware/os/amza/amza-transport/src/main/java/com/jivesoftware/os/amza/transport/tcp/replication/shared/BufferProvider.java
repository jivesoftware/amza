package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 */
public class BufferProvider {

    private final BlockingQueue<ByteBuffer> buffers;

    public BufferProvider(int bufferSize, int poolSize) {
        this.buffers = new LinkedBlockingQueue<>();
        for (int i = 0; i < poolSize; i++) {
            buffers.add(ByteBuffer.allocateDirect(bufferSize));
        }
    }

    public ByteBuffer acquire() {
        ByteBuffer buffer = null;
        while (buffer == null) {
            try {
                buffer = buffers.take();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        return buffer;
    }

    public boolean release(ByteBuffer byteBuffer) {
        byteBuffer.clear();
        return buffers.offer(byteBuffer);
    }
}
