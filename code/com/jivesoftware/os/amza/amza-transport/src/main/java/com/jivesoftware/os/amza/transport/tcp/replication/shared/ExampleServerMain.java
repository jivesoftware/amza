package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.transport.tcp.replication.messages.FrameableMessage;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FrameableSerializer;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.TableNameSerializer;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.TransactionSetSerializer;
import de.ruedigermoeller.serialization.FSTConfiguration;
import java.io.IOException;

/**
 *
 */
public class ExampleServerMain {

    public static void main(String[] args) {
        int bufferSize = 1024;
        int numWorkers = 10;
        int numBuffers = numWorkers * 2;

        BufferProvider bufferProvider = new BufferProvider(bufferSize, numBuffers, true);

        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());
        marshaller.registerSerializer(FrameableMessage.class, new FrameableSerializer());
        marshaller.registerSerializer(TableName.class, new TableNameSerializer());
        marshaller.registerSerializer(TransactionSet.class, new TransactionSetSerializer());

        MessageFramer framer = new MessageFramer(marshaller, new OpCodes().getPayloadRegistry());

        ServerRequestHandler handler = new ServerRequestHandler() {
            @Override
            public MessageFrame handleRequest(MessageFrame request) {
                //do stuff
                return null;
            }

            @Override
            public MessageFrame consumeSequence(long interactionId) {
                //do stuff
                return null;
            }
        };

        TcpServerInitializer initializer = new TcpServerInitializer();
        try {
            TcpServer server = initializer.initialize(new RingHost("localhost", 7777), numWorkers, bufferProvider, framer, handler);
            server.start();

            ///..... wait around

            server.stop();

        } catch (InterruptedException | IOException ex) {
            ex.printStackTrace(System.out);
            System.exit(-1);
        }
    }
}
