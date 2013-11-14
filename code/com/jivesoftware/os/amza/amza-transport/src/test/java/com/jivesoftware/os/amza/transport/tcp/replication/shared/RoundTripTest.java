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

import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayloadSerializer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import de.ruedigermoeller.serialization.FSTConfiguration;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 *
 */
public class RoundTripTest {

    private TcpServer server;
    private AtomicReference<RequestResponse> requestResponseMethod = new AtomicReference<>();
    private RingHost localHost = new RingHost("localhost", 7777);
    private TcpClientProvider tcpClientProvider;
    private OrderIdProvider idProvider;
    private int requestOpcode = 45;
    private int responseOpcode = 47;

    @BeforeTest
    public void setup() throws InterruptedException, IOException {
        int bufferSize = 1024;
        int numWorkers = 10;
        int numBuffers = numWorkers * 2;

        //setup server
        BufferProvider bufferProvider = new BufferProvider(bufferSize, numBuffers, true, 1000);

        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.createDefaultConfiguration());
        marshaller.registerSerializer(MessagePayload.class, new MessagePayloadSerializer());

        final Map<Integer, Class<? extends Serializable>> payloadRegistry = new HashMap<>();
        payloadRegistry.put(requestOpcode, String.class);
        payloadRegistry.put(responseOpcode, String.class);


        ApplicationProtocol applicationProtocol = new ApplicationProtocol() {
            private final AtomicLong ids = new AtomicLong();

            @Override
            public Response handleRequest(final Message request) {
                final RequestResponse method = requestResponseMethod.get();
                return new Response() {
                    @Override
                    public Message getMessage() {
                        return hasMessage() ? method.respondTo(request) : null;
                    }

                    @Override
                    public boolean isBlocking() {
                        return false;
                    }

                    @Override
                    public boolean hasMessage() {
                        return method != null;
                    }

                    @Override
                    public void writeTo(ResponseWriter responseWriter) {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public Message consumeSequence(long interactionId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Class<? extends Serializable> getOperationPayloadClass(int opCode) {
                return payloadRegistry.get(opCode);
            }

            @Override
            public long nextInteractionId() {
                return ids.incrementAndGet();
            }
        };

        MessageFramer framer = new MessageFramer(marshaller, applicationProtocol);

        TcpServerInitializer initializer = new TcpServerInitializer();
        server = initializer.initialize(localHost, numWorkers, bufferProvider, framer, applicationProtocol);
        server.start();


        //setup client
        bufferProvider = new BufferProvider(bufferSize, numBuffers, true, 1000);
        int connectionsPerHost = 2;
        int connectTimeoutMillis = 5000;
        int socketTimeoutMillis = 2000;
        tcpClientProvider = new TcpClientProvider(
                connectionsPerHost, connectTimeoutMillis, socketTimeoutMillis, bufferSize, bufferSize, bufferProvider, framer);

        idProvider = new OrderIdProvider() {
            private final AtomicLong id = new AtomicLong();

            @Override
            public long nextId() {
                return id.incrementAndGet();
            }
        };
    }

    @AfterTest
    public void tearDown() throws InterruptedException {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testMessageRoundTrip() throws Exception {
        final String sendText = "booya";
        final String returnText = "mmhmm";

        final AtomicBoolean requestReceived = new AtomicBoolean();

        requestResponseMethod.set(new RequestResponse() {
            @Override
            public Message respondTo(Message request) {
                String value = request.getPayload();
                requestReceived.set(value.equals(sendText));
                return new Message(request.getInteractionId(), responseOpcode, true, returnText);
            }
        });

        TcpClient client = tcpClientProvider.getClientForHost(localHost);


        try {
            long interactionId = idProvider.nextId();
            Message request = new Message(interactionId, requestOpcode, true, sendText);
            client.sendMessage(request);
            Message response = client.receiveMessage();
            Assert.assertTrue(requestReceived.get());
            Assert.assertNotNull(response);
            Assert.assertEquals(response.getInteractionId(), interactionId);
            Assert.assertEquals(response.getPayload(), returnText);
        } finally {
            tcpClientProvider.returnClient(client);
        }
    }

    private interface RequestResponse {

        Message respondTo(Message request);
    }
}
