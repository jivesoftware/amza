package com.jivesoftware.os.amza.transport.tcp.replication.serialization;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends OutputStream {

    private final ByteBuffer byteBuffer;

    public ByteBufferOutputStream(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public void write(int b) throws IOException {
        if (!byteBuffer.hasRemaining()) {
            throw new IOException("Overflowed the buffer");
        }
        byteBuffer.put((byte) b);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        if (byteBuffer.remaining() < length) {
            throw new IOException("Overflowed the buffer");
        }
        byteBuffer.put(bytes, offset, length);
    }
}
