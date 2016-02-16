package com.jivesoftware.os.amza.service.storage.filer;

import com.jivesoftware.os.amza.api.filer.IAppendOnly;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.service.filer.MultiAutoGrowingByteBufferBackedFiler;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class MemoryBackedWALFiler implements WALFiler {

    private final MultiAutoGrowingByteBufferBackedFiler filer;
    private final IAppendOnly appendOnly;
    private final AtomicLong size = new AtomicLong(0);

    public MemoryBackedWALFiler(MultiAutoGrowingByteBufferBackedFiler filer) {
        this.filer = filer;
        this.appendOnly = new IAppendOnly() {
            @Override
            public void write(byte[] b, int _offset, int _len) throws IOException {
                filer.write(b, _offset, _len);
                size.addAndGet(_len);
            }

            @Override
            public void flush(boolean fsync) throws IOException {
                filer.flush(fsync);
            }

            @Override
            public void close() throws IOException {
                filer.close();
            }

            @Override
            public Object lock() {
                return filer.lock();
            }

            @Override
            public long length() throws IOException {
                return size.get();
            }

            @Override
            public long getFilePointer() throws IOException {
                return filer.getFilePointer();
            }
        };
    }

    @Override
    public IReadable reader(IReadable current, long requiredLength, boolean fallBackToChannelReader, int bufferSize) throws IOException {
        if (current != null && current.length() >= requiredLength && current.length() <= filer.length()) {
            return current;
        }
        return filer.duplicateAll();
    }

    @Override
    public long length() throws IOException {
        return size.get();
    }

    @Override
    public void close() throws IOException {
        filer.close();
    }

    @Override
    public void truncate(long size) throws IOException {
        this.filer.setLength(size);
        this.size.set(size);
    }

    @Override
    public IAppendOnly appender() throws IOException {
        return appendOnly;
    }

    @Override
    public Object lock() {
        return this;
    }
}
