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

import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import java.nio.ByteBuffer;
import org.nustaq.serialization.FSTConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class MessageFramerTest {

    @Test
    public void testSerializationRoundTrip() throws Exception {
        BufferProvider bufferProvider = new BufferProvider(1024, 1, true, 1000);
        FstMarshaller fstMarshaller = new FstMarshaller(FSTConfiguration.createDefaultConfiguration());

        IndexReplicationProtocol protocol = new IndexReplicationProtocol(null, null);

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
