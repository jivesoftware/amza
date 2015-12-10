package com.jivesoftware.os.amza.shared.filer;

import java.nio.ByteBuffer;

/**
 *
 */
public class HeapByteBufferFactory implements ByteBufferFactory {

    @Override
    public ByteBuffer allocate(int index, long _size) {
        return ByteBuffer.allocate((int) _size);
    }

    @Override
    public ByteBuffer reallocate(int index, ByteBuffer oldBuffer, long newSize) {
        ByteBuffer newBuffer = allocate(index, newSize);
        if (oldBuffer != null) {
            long position = oldBuffer.position();
            oldBuffer.position(0);
            newBuffer.put(oldBuffer); // this assume we only grow. Blame Kevin :)
            newBuffer.position((int) position);
        }
        return newBuffer;
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public long length() {
        return 0;
    }

    @Override
    public long nextLength(int index, long oldLength, long position) {
        long newSize = oldLength * 2;
        while (newSize < position) {
            newSize *= 2;
        }
        return newSize;
    }
}
