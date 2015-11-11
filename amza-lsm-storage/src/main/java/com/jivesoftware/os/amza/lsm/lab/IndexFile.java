package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.IAppendOnly;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.shared.filer.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.amza.shared.filer.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class IndexFile extends RandomAccessFile implements IReadable {

    private static final long BUFFER_SEGMENT_SIZE = 1024L * 1024 * 1024;

    private final String fileName;
    private final boolean useMemMap;
    private final AtomicLong size;

    private final AutoGrowingByteBufferBackedFiler memMapFiler;
    private final AtomicLong memMapFilerLength = new AtomicLong(-1);

    public IndexFile(String name, String mode, boolean useMemMap) throws IOException {
        super(name, mode);
        this.fileName = name;
        this.useMemMap = useMemMap;
        this.size = new AtomicLong(super.length());
        if (useMemMap) {
            FileBackedMemMappedByteBufferFactory byteBufferFactory = new FileBackedMemMappedByteBufferFactory(new File(name), BUFFER_SEGMENT_SIZE);
            this.memMapFiler = new AutoGrowingByteBufferBackedFiler(-1L, BUFFER_SEGMENT_SIZE, byteBufferFactory);
        } else {
            this.memMapFiler = null;
        }
    }

    @Override
    public Object lock() {
        return this;
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
            return memMapFiler.duplicateAll();
        }
        synchronized (this) {
            if (size > memMapFilerLength.get()) {
                memMapFiler.seek(size);
                memMapFilerLength.set(size);
            }
        }
        return memMapFiler.duplicateAll();
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
        return super.read();
    }

    @Override
    public int read(byte b[]) throws IOException {
        return super.read(b);
    }

    @Override
    public int read(byte b[], int _offset, int _len) throws IOException {
        return super.read(b, _offset, _len);
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
