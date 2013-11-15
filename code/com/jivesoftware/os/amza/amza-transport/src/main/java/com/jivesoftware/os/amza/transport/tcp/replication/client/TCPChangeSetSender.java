package com.jivesoftware.os.amza.transport.tcp.replication.client;

import com.jivesoftware.os.amza.transport.tcp.replication.shared.SendReceiveChannel;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.SendReceiveChannelProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.messages.SendChangeSet;
import com.jivesoftware.os.amza.shared.ChangeSetSender;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.BufferProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.MessageFramer;
import java.nio.ByteBuffer;
import java.util.NavigableMap;

/**
 *
 */
public class TCPChangeSetSender implements ChangeSetSender {

    private final SendReceiveChannelProvider clientChannelProvider;
    private final MessageFramer messageFramer;
    private final BufferProvider bufferProvider;

    public TCPChangeSetSender(SendReceiveChannelProvider clientChannelProvider, MessageFramer messageFramer, BufferProvider bufferProvider) {
        this.clientChannelProvider = clientChannelProvider;
        this.messageFramer = messageFramer;
        this.bufferProvider = bufferProvider;
    }

    @Override
    public <K, V> void sendChangeSet(RingHost ringHost, TableName<K, V> mapName, NavigableMap<K, TimestampedValue<V>> changes) throws Exception {
        SendReceiveChannel channel = clientChannelProvider.getChannelForHost(ringHost);

        ByteBuffer sendBuff = bufferProvider.acquire();
        try {
            SendChangeSet sendChangeSet = new SendChangeSet(mapName, changes);
            messageFramer.toFrame(sendChangeSet, sendBuff);
            channel.send(sendBuff);
        } finally {
            bufferProvider.release(sendBuff);
        }
    }
}
