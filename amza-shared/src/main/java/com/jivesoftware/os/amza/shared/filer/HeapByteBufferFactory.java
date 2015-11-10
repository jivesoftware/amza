package com.jivesoftware.os.amza.shared.filer;

import java.nio.ByteBuffer;

/**
 *
 */
public class HeapByteBufferFactory implements ByteBufferFactory {

    @Override
    public ByteBuffer allocate(byte[] key, long _size) {
        return ByteBuffer.allocate((int) _size);
    }

    @Override
    public ByteBuffer reallocate(byte[] key, ByteBuffer oldBuffer, long newSize) {
        ByteBuffer newBuffer = allocate(key, newSize);
        if (oldBuffer != null) {
            long position = oldBuffer.position();
            oldBuffer.position(0);
            newBuffer.put(oldBuffer); // this assume we only grow. Blame Kevin :)
            newBuffer.position((int) position);
        }
        return newBuffer;
    }

    @Override
    public boolean exists(byte[] key) {
        return false;
    }
}
