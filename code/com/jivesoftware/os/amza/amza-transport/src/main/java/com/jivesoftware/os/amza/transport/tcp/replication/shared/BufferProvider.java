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
package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class BufferProvider {

    private final BlockingQueue<ByteBuffer> buffers;
    private final int bufferSize;
    private final boolean direct;
    private final long takeTimeoutMillis;

    public BufferProvider(int bufferSize, int poolSize, boolean direct, long takeTimeoutMillis) {
        this.buffers = new LinkedBlockingQueue<>();
        for (int i = 0; i < poolSize; i++) {
            buffers.add(direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize));
        }
        this.bufferSize = bufferSize;
        this.direct = direct;
        this.takeTimeoutMillis = takeTimeoutMillis;
    }

    public ByteBuffer acquire() {
        ByteBuffer buffer = null;
        while (buffer == null) {
            try {
                buffer = buffers.poll(takeTimeoutMillis, TimeUnit.MILLISECONDS);
                if (buffer == null) {
                    return ByteBuffer.allocate(bufferSize);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        return buffer;
    }

    public void release(ByteBuffer byteBuffer) {
        byteBuffer.clear();
        if (!direct || byteBuffer.isDirect()) {
            buffers.offer(byteBuffer);
        }
    }
}
