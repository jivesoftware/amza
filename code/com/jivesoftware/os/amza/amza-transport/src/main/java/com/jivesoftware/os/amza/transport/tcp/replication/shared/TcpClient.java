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

/**
 * TcpClient allows blocking request and response communication to a specific remote host. This class is not meant for use by multiple threads at once.
 */
public class TcpClient {

    private final SendReceiveChannel channel;
    private final BufferProvider bufferProvider;
    private final MessageFramer messageFramer;

    public TcpClient(SendReceiveChannel channel, BufferProvider bufferProvider, MessageFramer messageFramer) {
        this.channel = channel;
        this.bufferProvider = bufferProvider;
        this.messageFramer = messageFramer;
    }

    SendReceiveChannel getChannel() {
        return channel;
    }

    public void sendMessage(Message message) throws IOException {

        ByteBuffer sendBuff = bufferProvider.acquire();

        try {
            messageFramer.writeFrame(message, sendBuff);
            int position = sendBuff.position();
            int limit = sendBuff.limit();

            try {
                channel.send(sendBuff);
            } catch (IOException ioe) {
                channel.reconnect();

                sendBuff.position(position);
                sendBuff.limit(limit);

                channel.send(sendBuff);
            }
        } finally {
            bufferProvider.release(sendBuff);
        }
    }

    public Message receiveMessage() throws Exception {
        int read = 0;
        Message response = null;
        ByteBuffer readBuffer = bufferProvider.acquire();

        try {
            while (response == null && read >= 0) {
                read = channel.receive(readBuffer);
                response = messageFramer.readFrame(readBuffer);
            }

            return response;

        } finally {
            bufferProvider.release(readBuffer);
        }
    }
}
