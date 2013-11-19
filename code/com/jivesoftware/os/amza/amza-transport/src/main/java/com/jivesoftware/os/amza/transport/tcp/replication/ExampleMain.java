package com.jivesoftware.os.amza.transport.tcp.replication;

import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IdProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayloadSerializer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.BufferProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.MessageFramer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClientProvider;
import de.ruedigermoeller.serialization.FSTConfiguration;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class ExampleMain {

    public static void main(String[] args) {
        int connectionsPerHost = 2;
        int bufferSize = 10 * 1024;
        int numBuffers = 10;
        int connectTimeoutMillis = 5000;
        int socketTimeoutMillis = 2000;

        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());
        marshaller.registerSerializer(MessagePayload.class, new MessagePayloadSerializer());

        IndexReplicationProtocol protocol = new IndexReplicationProtocol(new IdProvider() {
            private final AtomicLong id = new AtomicLong();

            @Override
            public long nextId() {
                return id.incrementAndGet();
            }
        });

        MessageFramer framer = new MessageFramer(marshaller, protocol);
        BufferProvider bufferProvider = new BufferProvider(bufferSize, numBuffers, true);

        TcpClientProvider clientChannelProvider = new TcpClientProvider(
            connectionsPerHost, connectTimeoutMillis, socketTimeoutMillis, bufferSize, bufferSize, bufferProvider, framer);



        TcpChangeSetSender sender = new TcpChangeSetSender(clientChannelProvider, protocol);
        TcpChangeSetTaker taker = new TcpChangeSetTaker(clientChannelProvider, protocol);


        //send send send, take take take
    }
}
