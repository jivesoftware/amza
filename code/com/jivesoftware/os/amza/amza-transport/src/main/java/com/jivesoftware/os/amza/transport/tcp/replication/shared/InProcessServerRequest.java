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

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class InProcessServerRequest implements Comparable<InProcessServerRequest> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final MessageFramer messageFramer;
    private final BufferProvider bufferProvider;
    private final ByteBuffer readBuffer;
    private final AtomicReference<Message> message;

    public InProcessServerRequest(MessageFramer messageFramer, BufferProvider bufferProvider) {
        this.messageFramer = messageFramer;
        this.bufferProvider = bufferProvider;
        this.readBuffer = bufferProvider.acquire();
        this.message = new AtomicReference<>();
    }

    public boolean readRequest(SocketChannel channel) throws Exception {
        try {
            int read = channel.read(readBuffer);

            LOG.trace("Read {} bytes from connection to {}", read, channel.getRemoteAddress());

            if (read > 0) {
                //TODO this does not work - using the FrameableMessage interface rather than the concreate
                //class blows up with ClassNotFound. Need to work some fst magic or start sending opcodes to use
                //as a message class lookup key
                Message request = messageFramer.readFrame(readBuffer);
                if (request != null) {
                    message.set(request);
                    bufferProvider.release(readBuffer);
                }
                return true;
            } else {
                bufferProvider.release(readBuffer);
                return false;
            }
        } catch (Exception ex) {
            bufferProvider.release(readBuffer);
            throw ex;
        }
    }

    public Message getRequest() {
        return message.get();
    }

    public void releaseResources() {
        bufferProvider.release(readBuffer);
    }

    @Override
    public int compareTo(InProcessServerRequest o) {
        return Integer.compare(System.identityHashCode(this), System.identityHashCode(o));
    }
}
