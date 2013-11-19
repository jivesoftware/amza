package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import de.ruedigermoeller.serialization.FSTConfiguration;
import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class MessageFramerTest {

    @Test
    public void testSerializationRoundTrip() throws Exception {
        BufferProvider bufferProvider = new BufferProvider(1024, 1, true);
        FstMarshaller fstMarshaller = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());

        IndexReplicationProtocol protocol = new IndexReplicationProtocol(null);

        MessageFramer framer = new MessageFramer(fstMarshaller, protocol);

        ByteBuffer buffer = bufferProvider.acquire();
        try {
            Message request = new Message(23L, protocol.OPCODE_PUSH_CHANGESET, true);
            framer.writeFrame(request, buffer);

            //mimic write/read to/from channel

            buffer.limit(buffer.capacity());
            buffer.position(buffer.limit());

            Message received = framer.readFrame(buffer);

            Assert.assertNotNull(received);
            Assert.assertEquals(received.getInteractionId(), 23L);
            Assert.assertEquals(received.getOpCode(), protocol.OPCODE_PUSH_CHANGESET);
        } finally {
            bufferProvider.release(buffer);
        }

    }
}
