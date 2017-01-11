/*
 * Copyright 2015 JiveSoftware LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.service.replication.http.endpoints;

import com.jivesoftware.os.amza.api.filer.IWriteable;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import java.io.IOException;
import org.glassfish.jersey.server.ChunkedOutput;

/**
 * @author jonathan.colt
 */
public class ChunkedOutputFiler implements IWriteable {

    private final int bufferSize;
    private final HeapFiler filer;
    private final ChunkedOutput<byte[]> chunkedOutput;

    public ChunkedOutputFiler(int bufferSize, ChunkedOutput<byte[]> chunkedOutput) {
        this.bufferSize = bufferSize;
        this.filer = new HeapFiler(bufferSize);
        this.chunkedOutput = chunkedOutput;
    }

    @Override
    public void write(byte[] b, int _offset, int _len) throws IOException {
        filer.write(b, _offset, _len);
        flushChunk(false);
    }

    @Override
    public void flush(boolean fsync) throws IOException {
        flushChunk(true);
    }

    @Override
    public void close() throws IOException {
        flushChunk(true);
        filer.close();
        if (!chunkedOutput.isClosed()) {
            chunkedOutput.close();
        }
    }

    private void flushChunk(boolean force) throws IOException {
        long fp = filer.getFilePointer();
        if (force && fp > 0 || fp >= bufferSize) {
            chunkedOutput.write(filer.copyUsedBytes());
            filer.reset();
        }
    }

    @Override
    public Object lock() {
        return chunkedOutput;
    }

    @Override
    public void seek(long position) throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public long length() throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public long getFilePointer() throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }

}
