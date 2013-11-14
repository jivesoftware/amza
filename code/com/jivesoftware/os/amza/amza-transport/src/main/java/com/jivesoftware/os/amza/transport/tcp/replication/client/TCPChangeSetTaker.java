package com.jivesoftware.os.amza.transport.tcp.replication.client;

import com.jivesoftware.os.amza.shared.ChangeSetTaker;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.FstMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.ReadBufferProvider;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import java.nio.ByteBuffer;

/**
 *
 */
public class TCPChangeSetTaker implements ChangeSetTaker {

    private final ClientChannelProvider clientChannelProvider;
    private final ReadBufferProvider readBufferProvider;
    private final FstMarshaller fstMarshaller;

    public TCPChangeSetTaker(ClientChannelProvider clientChannelProvider, ReadBufferProvider readBufferProvider, FstMarshaller fstMarshaller) {
        this.clientChannelProvider = clientChannelProvider;
        this.readBufferProvider = readBufferProvider;
        this.fstMarshaller = fstMarshaller;
    }

    @Override
    public <K, V> void take(RingHost ringHost, TableName<K, V> tableName, long transationId, final TransactionSetStream transactionSetStream)
        throws Exception {
        ClientChannel channel = clientChannelProvider.getChannelForHost(ringHost);

        CallbackStream<byte[]> messageStream = new CallbackStream<byte[]>() {
            @Override
            public byte[] callback(byte[] value) throws Exception {
                if (value != null) {
                    TransactionSet transactionSet = fstMarshaller.deserialize(value, TransactionSet.class);
                    return transactionSetStream.stream(transactionSet) ? value : null;
                } else {
                    return null;
                }
            }
        };

        ByteBuffer readBuffer = readBufferProvider.acquire();
        try {
            channel.receive(messageStream, readBuffer);
        } finally {
            readBufferProvider.release(readBuffer);
        }

    }
}
