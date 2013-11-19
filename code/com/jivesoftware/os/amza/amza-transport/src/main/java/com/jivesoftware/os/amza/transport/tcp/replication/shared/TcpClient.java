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
