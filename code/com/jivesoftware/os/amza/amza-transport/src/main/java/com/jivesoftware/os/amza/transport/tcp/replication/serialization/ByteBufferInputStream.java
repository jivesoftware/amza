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
package com.jivesoftware.os.amza.transport.tcp.replication.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 *
 */
public class ByteBufferInputStream extends InputStream {

    private final ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public int read() throws IOException {
        if (buffer.hasRemaining()) {
            return buffer.get();
        }
        return -1;
    }

    public int read(byte[] bytes, int offset, int length) throws IOException {
        if (buffer.hasRemaining()) {
            length = Math.min(length, buffer.remaining());
            buffer.get(bytes, offset, length);
            return length;
        }

        return -1;
    }

    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }
}
