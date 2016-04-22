/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.amza.service.filer;

import com.jivesoftware.os.amza.api.filer.IFiler;
import com.jivesoftware.os.filer.io.DirectBufferCleaner;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 *
 * @author jonathan.colt
 */
public class ByteBufferBackedFiler implements IFiler {

    final ByteBuffer buffer;

    public ByteBufferBackedFiler(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public Object lock() {
        return this;
    }

    public ByteBufferBackedFiler duplicate() {
        return new ByteBufferBackedFiler(buffer.duplicate());
    }

    @Override
    public void seek(long position) throws IOException {
        buffer.position((int) position); // what a pain! limited to an int!
    }

    @Override
    public long skip(long position) throws IOException {
        int p = buffer.position();
        p += position;
        buffer.position(p);
        return p;
    }

    @Override
    public long length() throws IOException {
        return buffer.capacity();
    }

    @Override
    public void setLength(long len) throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public long getFilePointer() throws IOException {
        return buffer.position();
    }

    @Override
    public void eof() throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public void flush(boolean fsync) throws IOException {
        if (fsync && buffer instanceof MappedByteBuffer) {
            try {
                ((MappedByteBuffer) buffer).force();
            } catch (UnsupportedOperationException e) {
                // HMMM
            }
        }
    }

    @Override
    public int read() throws IOException {
        int remaining = buffer.remaining();
        if (remaining == 0) {
            return -1;
        }
        byte b = buffer.get();
        return b & 0xFF;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int _offset, int _len) throws IOException {
        int remaining = buffer.remaining();
        if (remaining == 0) {
            return -1;
        }
        int count = Math.min(_len, remaining);
        buffer.get(b, _offset, count);
        return count;
    }

    @Override
    public void close() throws IOException {
        DirectBufferCleaner.clean(buffer);
    }

    @Override
    public void write(byte[] b, int _offset, int _len) throws IOException {
        buffer.put(b, _offset, _len);
    }

}
