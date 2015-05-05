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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 *
 */
public class InProcessServerResponse implements Comparable<InProcessServerResponse> {

    private final BufferProvider bufferProvider;
    private final ByteBuffer writeBuffer;
    private final long interactionId;
    private final boolean lastInSequence;

    public InProcessServerResponse(MessageFramer messageFramer, BufferProvider bufferProvider, Message response) throws IOException {
        this.bufferProvider = bufferProvider;
        this.writeBuffer = bufferProvider.acquire();
        this.interactionId = response.getInteractionId();
        this.lastInSequence = response.isLastInSequence();

        messageFramer.writeFrame(response, writeBuffer);
    }

    public long getInteractionId() {
        return interactionId;
    }

    public boolean isLastInSequence() {
        return lastInSequence;
    }

    boolean writeResponse(SocketChannel channel) throws IOException {
        try {
            channel.write(writeBuffer);
            if (writeBuffer.remaining() == 0) {
                bufferProvider.release(writeBuffer);
                return true;
            } else {
                return false;
            }
        } catch (Exception ex) {
            bufferProvider.release(writeBuffer);
            throw ex;
        }
    }

    public void releaseResources() {
        bufferProvider.release(writeBuffer);
    }

    @Override
    public int compareTo(InProcessServerResponse o) {
        return Integer.compare(System.identityHashCode(this), System.identityHashCode(o));
    }
}
