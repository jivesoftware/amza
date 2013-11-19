package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.transport.tcp.replication.messages.ChangeSetRequest;
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
        MessageFramer framer = new MessageFramer(fstMarshaller);

        ByteBuffer buffer = bufferProvider.acquire();
        try {
            ChangeSetRequest request = new ChangeSetRequest();
            framer.toFrame(request, buffer);

            //mimic write/read to/from channel

            buffer.limit(buffer.capacity());
            buffer.position(buffer.limit());

            ChangeSetRequest received = framer.fromFrame(buffer, ChangeSetRequest.class);

            Assert.assertNotNull(received);
            Assert.assertTrue(received.getClass().equals(ChangeSetRequest.class));
        } finally {
            bufferProvider.release(buffer);
        }

    }
}
