package com.jivesoftware.os.amza.transport.tcp.replication.client;

import com.jivesoftware.os.amza.transport.tcp.replication.client.TCPChangeSetSender;
import com.jivesoftware.os.amza.transport.tcp.replication.client.TCPChangeSetTaker;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.BufferProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.FrameableMessage;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.FrameableSerializer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.FstMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.MessageFramer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.SendReceiveChannelProvider;
import de.ruedigermoeller.serialization.FSTConfiguration;

/**
 *
 */
public class ExampleMain {

    public static void main(String[] args) {
        int bufferSize = 10 * 1024;
        int numBuffers = 10;
        int connectTimeoutMillis = 5000;
        int socketTimeoutMillis = 2000;

        BufferProvider bufferProvider = new BufferProvider(bufferSize, numBuffers);
        SendReceiveChannelProvider clientChannelProvider = new SendReceiveChannelProvider(
            connectTimeoutMillis, socketTimeoutMillis, bufferSize, bufferSize);

        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());
        marshaller.registerSerializer(FrameableMessage.class, new FrameableSerializer());

        MessageFramer framer = new MessageFramer(marshaller);

        TCPChangeSetSender sender = new TCPChangeSetSender(clientChannelProvider, framer, bufferProvider);
        TCPChangeSetTaker taker = new TCPChangeSetTaker(clientChannelProvider, framer, bufferProvider);


        //send send send

        clientChannelProvider.closeAll();

    }
}
