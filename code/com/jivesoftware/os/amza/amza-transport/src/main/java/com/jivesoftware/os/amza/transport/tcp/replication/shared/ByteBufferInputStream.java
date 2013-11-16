package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 *
 */
public class ByteBufferInputStream extends InputStream {

    private final ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public int read() throws IOException {
        if (buffer.hasRemaining()) {
            return buffer.get();
        }
        return -1;
    }

    public int read(byte[] bytes, int offset, int length) throws IOException {
        if (buffer.hasRemaining()) {
            length = Math.min(length, buffer.remaining());
            buffer.get(bytes, offset, length);
            return length;
        }

        return -1;
    }

    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }
}
