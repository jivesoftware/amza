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

import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayloadSerializer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.BufferProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.MessageFramer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClientProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
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

        IndexReplicationProtocol protocol = new IndexReplicationProtocol(null, new OrderIdProvider() {
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
