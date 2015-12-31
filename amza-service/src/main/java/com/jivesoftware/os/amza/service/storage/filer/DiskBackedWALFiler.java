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

import com.jivesoftware.os.amza.api.filer.IFiler;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.shared.filer.ByteBufferBackedFiler;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class DiskBackedWALFiler extends RandomAccessFile implements WALFiler, IFiler {

    private final String fileName;
    private final boolean useMemMap;
    private final AtomicLong size;

    private final AtomicReference<ByteBufferBackedFiler> memMapFiler = new AtomicReference<>();
    private final AtomicLong memMapFilerLength = new AtomicLong(-1);

    public DiskBackedWALFiler(String name, String mode, boolean useMemMap) throws IOException {
        super(name, mode);
        this.fileName = name;
        this.useMemMap = useMemMap;
        this.size = new AtomicLong(super.length());
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public IReadable fileChannelFiler() {
        final FileChannel channel = getChannel();
        return new DiskBackedWALFilerChannelReader(this, channel);
    }

    @Override
    public IReadable bestFiler(IReadable current, long size) throws IOException {
        if (current != null && current.length() >= size) {
            return current;
        }
        if (!useMemMap) {
            return fileChannelFiler();
        }
        if (memMapFilerLength.get() >= size) {
            return memMapFiler.get().duplicate();
        }
        synchronized (this) {
            if (size <= memMapFilerLength.get()) {
                return memMapFiler.get().duplicate();
            }
            final FileChannel channel = getChannel();
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
    public Object lock() {
        return this;
    }

    @Override
    public long skip(long position) throws IOException {
        throw new UnsupportedOperationException("No skipping! Call fileChannelFiler() to read!");
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
        super.write(b);
        size.incrementAndGet();
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

    @Override
    public void eof() throws IOException {
        setLength(getFilePointer());
        size.set(super.length());
    }

    @Override
    public void flush(boolean fsync) throws IOException {
        if (fsync) {
            getFD().sync();
        }
    }
}
