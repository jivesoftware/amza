package com.jivesoftware.os.amza.transport.tcp.replication.client;

import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.transport.tcp.replication.messages.FrameableMessage;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FrameableSerializer;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.TableNameSerializer;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.TransactionSetSerializer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.BufferProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.MessageFramer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClientProvider;
import de.ruedigermoeller.serialization.FSTConfiguration;

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
        marshaller.registerSerializer(FrameableMessage.class, new FrameableSerializer());
        marshaller.registerSerializer(TableName.class, new TableNameSerializer());
        marshaller.registerSerializer(TransactionSet.class, new TransactionSetSerializer());

        MessageFramer framer = new MessageFramer(marshaller);

        BufferProvider bufferProvider = new BufferProvider(bufferSize, numBuffers);
        TcpClientProvider clientChannelProvider = new TcpClientProvider(
            connectionsPerHost, connectTimeoutMillis, socketTimeoutMillis, bufferSize, bufferSize, bufferProvider, framer);

        TcpChangeSetSender sender = new TcpChangeSetSender(clientChannelProvider);
        TcpChangeSetTaker taker = new TcpChangeSetTaker(clientChannelProvider);


        //send send send, take take take
    }
}
