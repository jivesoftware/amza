package com.jivesoftware.os.amza.service.storage.filer;

import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.IWriteable;
import com.jivesoftware.os.amza.shared.filer.AutoGrowingByteBufferBackedFiler;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class MemoryBackedWALFiler implements WALFiler, IReadable, IWriteable {

    private final AutoGrowingByteBufferBackedFiler filer;
    private final AtomicLong size = new AtomicLong(0);

    public MemoryBackedWALFiler(AutoGrowingByteBufferBackedFiler filer) {
        this.filer = filer;
    }

    @Override
    public IReadable fileChannelFiler() throws IOException {
        return filer.duplicateAll();
    }

    @Override
    public IReadable bestFiler(IReadable current, long boundaryFp) throws IOException {
        if (current != null && current.length() >= boundaryFp) {
            return current;
        }
        return filer.duplicateAll();
    }

    @Override
    public int read() throws IOException {
        return filer.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return filer.read(b);
    }

    @Override
    public int read(byte[] b, int _offset, int _len) throws IOException {
        return filer.read(b, _offset, _len);
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
    public void seek(long position) throws IOException {
        filer.seek(position);
    }

    @Override
    public long length() throws IOException {
        return size.get();
    }

    @Override
    public long getFilePointer() throws IOException {
        return filer.getFilePointer();
    }

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
    public void eof() throws IOException {
        filer.eof();
    }
}
