package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.IAppendOnly;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.shared.filer.ByteBufferBackedFiler;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author jonathan.colt
 */
public class IndexFile extends RandomAccessFile {

    private final String fileName;
    private final boolean useMemMap;
    private final AtomicLong size;

    private final AtomicReference<ByteBufferBackedFiler> memMapFiler = new AtomicReference<>();
    private final AtomicLong memMapFilerLength = new AtomicLong(-1);

    public IndexFile(String name, String mode, boolean useMemMap) throws IOException {
        super(name, mode);
        this.fileName = name;
        this.useMemMap = useMemMap;
        this.size = new AtomicLong(super.length());
    }

    public String getFileName() {
        return fileName;
    }

    public IReadable fileChannelFiler() {
        FileChannel channel = getChannel();
        return new IndexFilerChannelReader(this, channel);
    }

    public IReadable fileChannelMemMapFiler(long size) throws IOException {
        if (!useMemMap) {
            return null;
        }
        if (size <= memMapFilerLength.get()) {
            return memMapFiler.get().duplicate();
        }
        synchronized (this) {
            if (size <= memMapFilerLength.get()) {
                return memMapFiler.get().duplicate();
            }
            FileChannel channel = getChannel();
            long newLength = length();
            // TODO handle larger files
            if (newLength >= Integer.MAX_VALUE) {
                return null;
            }
            ByteBufferBackedFiler newFiler = new ByteBufferBackedFiler(channel.map(FileChannel.MapMode.READ_ONLY, 0, (int) newLength));
            memMapFiler.set(newFiler);
            memMapFilerLength.set(newLength);
            return newFiler.duplicate();
        }
    }

    public IAppendOnly fileChannelWriter(int bufferSize) throws IOException {

        HeapFiler filer = new HeapFiler(bufferSize);
        seek(0);
        return new IAppendOnly() {
            private long flushedFp = 0;

            @Override
            public void write(byte[] b, int _offset, int _len) throws IOException {
                filer.write(b, _offset, _len);
                if (filer.length() > bufferSize) {
                    flush(false);
                }
            }

            @Override
            public void flush(boolean fsync) throws IOException {
                long length = filer.length();
                flushedFp += length;
                IndexFile.this.write(filer.leakBytes(), 0, (int) length);
                filer.reset();
            }

            @Override
            public void close() throws IOException {
                filer.reset();
            }

            @Override
            public Object lock() {
                return IndexFile.this;
            }

            @Override
            public long length() throws IOException {
                return flushedFp + filer.getFilePointer();
            }

            @Override
            public long getFilePointer() throws IOException {
                return flushedFp + filer.getFilePointer();
            }
        };

    }

    void addToSize(long amount) {
        size.addAndGet(amount);
    }

    @Override
    public String toString() {
        return "DiskBackedWALFiler{"
            + "fileName=" + fileName
            + ", useMemMap=" + useMemMap
            + ", size=" + size
            + ", memMapFiler=" + memMapFiler
            + ", memMapFilerLength=" + memMapFilerLength
            + '}';
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    public void seek(long _fp) throws IOException {
        super.seek(_fp);
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException("No reading! Call fileChannelFiler() to read!");
    }

    @Override
    public int read(byte b[]) throws IOException {
        throw new UnsupportedOperationException("No reading! Call fileChannelFiler() to read!");
    }

    @Override
    public int read(byte b[], int _offset, int _len) throws IOException {
        throw new UnsupportedOperationException("No reading! Call fileChannelFiler() to read!");
    }

    @Override
    public long length() throws IOException {
        return size.get();
    }

    @Override
    public void write(int b) throws IOException {
        size.incrementAndGet();
        super.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (b != null) {
            super.write(b);
            size.addAndGet(b.length);
        }
    }

    @Override
    public void write(byte[] b, int _offset, int _len) throws IOException {
        super.write(b, _offset, _len);
        size.addAndGet(_len);
    }

    public void eof() throws IOException {
        setLength(getFilePointer());
    }

}
