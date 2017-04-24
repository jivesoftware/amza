/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.service.storage.filer;

import com.jivesoftware.os.amza.api.filer.IAppendOnly;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.service.filer.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.amza.api.filer.HeapFiler;
import com.jivesoftware.os.amza.service.filer.SingleAutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DiskBackedWALFiler implements WALFiler {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final long BUFFER_SEGMENT_SIZE = 1024L * 1024 * 1024;

    private final String fileName;
    private final String mode;
    private RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private final boolean useMemMap;
    private final AtomicLong size;
    private final IAppendOnly appendOnly;

    private SingleAutoGrowingByteBufferBackedFiler memMapFiler;
    private final AtomicLong memMapFilerLength = new AtomicLong(-1);

    private final Object fileLock = new Object();
    private final Object memMapLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public DiskBackedWALFiler(String fileName, String mode, boolean useMemMap, int appendBufferSize) throws IOException {
        this.fileName = fileName;
        this.mode = mode;
        this.useMemMap = useMemMap;
        this.randomAccessFile = new RandomAccessFile(fileName, mode);
        this.channel = randomAccessFile.getChannel();
        this.size = new AtomicLong(randomAccessFile.length());
        this.appendOnly = createAppendOnly(appendBufferSize);
        this.memMapFiler = createMemMap();
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public IReadable reader(IReadable current, long requiredLength, boolean fallBackToChannelReader, int bufferSize) throws IOException {
        if (!useMemMap) {
            if (current != null) {
                return current;
            } else {
                DiskBackedWALFilerChannelReader reader = new DiskBackedWALFilerChannelReader(this, channel, closed);
                return bufferSize > 0 ? new HeapBufferedReadable(reader, bufferSize) : reader;
            }
        }

        // quick check to see if the current filer is big enough
        long length = size.get();
        if (current != null && current.length() <= length && current.length() >= requiredLength) {
            return current;
        }

        synchronized (memMapLock) {
            // double check current filer
            length = size.get();
            if (current != null && current.length() <= length && current.length() >= requiredLength) {
                return current;
            }
            if (fallBackToChannelReader && memMapFilerLength.get() < requiredLength) {
                // mmap is too small, fall back to channel reader
                DiskBackedWALFilerChannelReader reader = new DiskBackedWALFilerChannelReader(this, channel, closed);
                return bufferSize > 0 ? new HeapBufferedReadable(reader, bufferSize) : reader;
            }
            memMapFiler.seek(length);
            memMapFilerLength.set(length);
        }
        return memMapFiler.duplicateAll();
    }

    @Override
    public IAppendOnly appender() throws IOException {
        return appendOnly;
    }

    private IAppendOnly createAppendOnly(int bufferSize) throws IOException {
        HeapFiler filer = bufferSize > 0 ? new HeapFiler(bufferSize) : null;
        randomAccessFile.seek(size.get());
        return new IAppendOnly() {

            @Override
            public void write(byte[] b, int _offset, int _len) throws IOException {
                if (filer != null) {
                    filer.write(b, _offset, _len);
                    if (filer.length() > bufferSize) {
                        flush(false);
                    }
                } else {
                    DiskBackedWALFiler.this.write(b, _offset, _len);
                }
            }

            @Override
            public void flush(boolean fsync) throws IOException {
                if (filer != null && filer.length() > 0) {
                    long length = filer.length();
                    DiskBackedWALFiler.this.write(filer.leakBytes(), 0, (int) length);
                    filer.reset();
                }
                DiskBackedWALFiler.this.flush(fsync);
            }

            @Override
            public void close() throws IOException {
                closed.compareAndSet(false, true);
                if (filer != null) {
                    filer.reset();
                }
            }

            @Override
            public Object lock() {
                return DiskBackedWALFiler.this;
            }

            @Override
            public long length() throws IOException {
                return DiskBackedWALFiler.this.length() + (filer != null ? filer.length() : 0);
            }

            @Override
            public long getFilePointer() throws IOException {
                return length();
            }
        };
    }

    private SingleAutoGrowingByteBufferBackedFiler createMemMap() throws IOException {
        if (useMemMap) {
            FileBackedMemMappedByteBufferFactory byteBufferFactory = new FileBackedMemMappedByteBufferFactory(new File(fileName), BUFFER_SEGMENT_SIZE);
            return new SingleAutoGrowingByteBufferBackedFiler(-1L, BUFFER_SEGMENT_SIZE, byteBufferFactory);
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return "DiskBackedWALFiler{"
            + "fileName=" + fileName
            + ", useMemMap=" + useMemMap
            + ", size=" + size
            + '}';
    }

    @Override
    public void close() throws IOException {
        closed.compareAndSet(false, true);
        randomAccessFile.close();
        if (memMapFiler != null) {
            memMapFiler.close();
        }
    }

    @Override
    public long length() throws IOException {
        return size.get();
    }

    private void write(byte[] b, int _offset, int _len) throws IOException {
        while (!closed.get()) {
            try {
                randomAccessFile.write(b, _offset, _len);
                size.addAndGet(_len);
                break;
            } catch (ClosedChannelException e) {
                ensureOpen();
            }
        }
    }

    private void flush(boolean fsync) throws IOException {
        if (fsync) {
            while (!closed.get()) {
                try {
                    randomAccessFile.getFD().sync();
                    break;
                } catch (ClosedChannelException e) {
                    ensureOpen();
                }
            }
        }
    }

    @Override
    public void truncate(long truncatedSize) throws IOException {
        // should only be called with a write AND a read lock
        while (!closed.get()) {
            try {
                randomAccessFile.setLength(truncatedSize);
                size.set(randomAccessFile.length());

                synchronized (memMapLock) {
                    memMapFiler = createMemMap();
                    memMapFilerLength.set(-1);
                }
                break;
            } catch (ClosedChannelException e) {
                ensureOpen();
            }
        }
    }

    private void ensureOpen() throws IOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException();
        }
        if (!channel.isOpen()) {
            if (closed.get()) {
                throw new IOException("The WAL filer has been closed");
            }
            synchronized (fileLock) {
                if (!channel.isOpen()) {
                    LOG.warn("File channel is closed and must be reopened for {}", fileName);
                    try {
                        randomAccessFile.close();
                    } catch (IOException e) {
                        LOG.error("Failed to close existing random access file while reacquiring channel");
                    }
                    randomAccessFile = new RandomAccessFile(fileName, mode);
                    channel = randomAccessFile.getChannel();
                    randomAccessFile.seek(size.get());
                }
            }
        }
    }

    FileChannel getFileChannel() throws IOException {
        ensureOpen();
        return channel;
    }

    @Override
    public Object lock() {
        return this;
    }
}
