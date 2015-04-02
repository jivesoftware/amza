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
package com.jivesoftware.os.amza.storage.filer;

import com.jivesoftware.os.amza.shared.filer.ByteBufferBackedFiler;
import com.jivesoftware.os.amza.shared.filer.IFiler;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

public class WALFiler extends RandomAccessFile implements IFiler {

    public static long totalFilesOpenCount;
    public static long totalReadByteCount;
    public static long totalWriteByteCount;
    public static long totalSeeksCount;
    public long readByteCount;
    public long writeByteCount;

    private final String fileName;
    private final AtomicLong size;

    public WALFiler(String name, String mode) throws IOException {
        super(name, mode);
        this.fileName = name;
        this.size = new AtomicLong(super.length());
    }

    public WALFilerChannelReader fileChannelFiler() {
        final FileChannel channel = getChannel();
        return new WALFilerChannelReader(this, channel);
    }

    public IFiler fileChannelMemMapFiler() throws IOException {
        final FileChannel channel = getChannel();
        // TODO handle larger files;
        return new ByteBufferBackedFiler(channel.map(FileChannel.MapMode.READ_ONLY, 0, (int)length()));
    }

    @Override
    public String toString() {
        try {
            return "R:" + (readByteCount / 1024) + "kb W:" + (writeByteCount / 1024) + "kb " + fileName + " " + (length() / 1024) + "kb";
        } catch (IOException x) {
            return "R:" + (readByteCount / 1024) + "kb W:" + (writeByteCount / 1024) + "kb " + fileName;
        }
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
        totalSeeksCount++;
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
        writeByteCount++;
        totalWriteByteCount++;
        size.incrementAndGet();
        super.write(b);
    }

    @Override
    public void write(byte b[]) throws IOException {
        if (b != null) {
            writeByteCount += b.length;
            totalWriteByteCount += b.length;
        }
        super.write(b);
        size.addAndGet(b.length);
    }

    @Override
    public void write(byte b[], int _offset, int _len) throws IOException {
        super.write(b, _offset, _len);
        size.addAndGet(_len);
        writeByteCount += _len;
        totalWriteByteCount += _len;
    }

    @Override
    public void eof() throws IOException {
        setLength(getFilePointer());
    }

    @Override
    public void flush() throws IOException {
    }
}
