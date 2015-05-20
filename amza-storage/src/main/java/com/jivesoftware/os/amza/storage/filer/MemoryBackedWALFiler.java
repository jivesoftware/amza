package com.jivesoftware.os.amza.storage.filer;

import com.jivesoftware.os.amza.shared.filer.IReadable;
import com.jivesoftware.os.amza.shared.filer.IWriteable;
import com.jivesoftware.os.amza.shared.filer.MemoryFiler;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class MemoryBackedWALFiler implements WALFiler, IReadable, IWriteable {

    private final MemoryFiler filer;

    public MemoryBackedWALFiler(MemoryFiler filer) {
        this.filer = filer;
    }

    @Override
    public IReadable fileChannelFiler() throws IOException {
        return new MemoryFiler(filer.leakBytes());
    }

    @Override
    public IReadable fileChannelMemMapFiler(long boundaryFp) throws IOException {
        return new MemoryFiler(filer.leakBytes());
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
        return filer.length();
    }

    @Override
    public long getFilePointer() throws IOException {
        return filer.getFilePointer();
    }

    @Override
    public void write(int b) throws IOException {
        filer.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        filer.write(b);
    }

    @Override
    public void write(byte[] b, int _offset, int _len) throws IOException {
        filer.write(b, _offset, _len);
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
