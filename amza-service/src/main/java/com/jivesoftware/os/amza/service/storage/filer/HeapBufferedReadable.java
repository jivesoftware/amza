package com.jivesoftware.os.amza.service.storage.filer;

import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.HeapFiler;
import java.io.IOException;

/**
 *
 */
public class HeapBufferedReadable implements IReadable {

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

    public HeapFiler ensureHeapFiler(long position, int needed) throws IOException {
        if (buffer != null && position >= startOfBufferFp && position + needed < (startOfBufferFp + buffer.length())) {
            buffer.seek(position - startOfBufferFp);
            return buffer;
        } else {
            backingReadable.seek(position);
            long remaining = length() - position;
            int nextBufferSize = Math.max(bufferSize, needed);
            int used = (int) Math.min(remaining, (long) nextBufferSize);
            if (buffer == null || needed > buffer.leakBytes().length) {
                byte[] bytes = new byte[nextBufferSize];
                backingReadable.read(bytes, 0, used);
                buffer = HeapFiler.fromBytes(bytes, used);
            } else {
                buffer.reset(used);
                backingReadable.read(buffer.leakBytes(), 0, used);
            }
            startOfBufferFp = position;
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
