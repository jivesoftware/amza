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
package com.jivesoftware.os.amza.transport.tcp.replication;

import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayloadSerializer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.ApplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.BufferProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.MessageFramer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpServer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpServerInitializer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import de.ruedigermoeller.serialization.FSTConfiguration;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

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
        marshaller.registerSerializer(MessagePayload.class, new MessagePayloadSerializer());

        ApplicationProtocol protocol = new IndexReplicationProtocol(null, new OrderIdProvider() {
            private final AtomicLong id = new AtomicLong();

            @Override
            public long nextId() {
                return id.incrementAndGet();
            }
        });

        MessageFramer framer = new MessageFramer(marshaller, protocol);

        TcpServerInitializer initializer = new TcpServerInitializer();
        try {
            TcpServer server = initializer.initialize(new RingHost("localhost", 7777), numWorkers, bufferProvider, framer, protocol);
            server.start();

            ///..... serve serve serve

            server.stop();

        } catch (InterruptedException | IOException ex) {
            ex.printStackTrace(System.out);
            System.exit(-1);
        }
    }
}
