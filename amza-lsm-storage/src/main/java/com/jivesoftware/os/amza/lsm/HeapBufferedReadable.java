package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
class HeapBufferedReadable implements IReadable {

    private final IReadable backingReadable;
    private final int bufferSize;
    private long startOfBufferFp;
    private HeapFiler buffer;

    public HeapBufferedReadable(IReadable backingReadable, int bufferSize) {
        this.backingReadable = backingReadable;
        this.bufferSize = bufferSize;
    }

    @Override
    public long getFilePointer() throws IOException {
        if (buffer == null) {
            return backingReadable.getFilePointer();
        }
        return startOfBufferFp + buffer.getFilePointer();
    }

    HeapFiler ensureHeapFiler(long position, int needed) throws IOException {
        if (buffer != null && position >= startOfBufferFp && position + needed < (startOfBufferFp + buffer.length())) {
            buffer.seek(position - startOfBufferFp);
            return buffer;
        } else {
            backingReadable.seek(position);
            long remaining = length() - position;
            byte[] bytes = new byte[Math.min((int) remaining, Math.max(bufferSize, needed))];
            backingReadable.read(bytes);
            startOfBufferFp = position;
            buffer = new HeapFiler(bytes);
            return buffer;
        }
    }

    @Override
    public void seek(long position) throws IOException {
        ensureHeapFiler(position, 0);
    }

    @Override
    public int read() throws IOException {
        return ensureHeapFiler(getFilePointer(), 1).read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return ensureHeapFiler(getFilePointer(), b.length).read(b);
    }

    @Override
    public int read(byte[] b, int _offset, int _len) throws IOException {
        return ensureHeapFiler(getFilePointer(), _len).read(b, _offset, _len);
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            buffer.close();
        }
        backingReadable.close();
    }

    @Override
    public Object lock() {
        return backingReadable;
    }

    @Override
    public long length() throws IOException {
        return backingReadable.length();
    }

}
