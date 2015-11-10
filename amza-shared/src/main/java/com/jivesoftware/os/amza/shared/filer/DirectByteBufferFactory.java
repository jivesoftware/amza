package com.jivesoftware.os.amza.shared.filer;

import com.jivesoftware.os.filer.io.*;
import java.nio.ByteBuffer;

/**
 *
 */
public class DirectByteBufferFactory implements ByteBufferFactory {

    @Override
    public ByteBuffer allocate(byte[] key, long _size) {
        return ByteBuffer.allocateDirect((int) _size);
    }

    @Override
    public ByteBuffer reallocate(byte[] key, ByteBuffer oldBuffer, long newSize) {
        ByteBuffer newBuffer = allocate(key, newSize);
        if (oldBuffer != null) {
            oldBuffer.position(0);
            newBuffer.put(oldBuffer); // this assume we only grow. Blame Kevin :)
            newBuffer.position(0);
        }
        return newBuffer;
    }

    @Override
    public boolean exists(byte[] key) {
        return false;
    }
}