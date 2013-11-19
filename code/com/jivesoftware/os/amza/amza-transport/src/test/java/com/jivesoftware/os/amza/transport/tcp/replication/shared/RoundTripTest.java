package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.transport.tcp.replication.messages.FrameableMessage;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FrameableSerializer;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import de.ruedigermoeller.serialization.FSTConfiguration;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;
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
    private IdProvider idProvider;

    @BeforeTest
    public void setup() throws InterruptedException, IOException {
        int bufferSize = 1024;
        int numWorkers = 10;
        int numBuffers = numWorkers * 2;

        //setup server
        BufferProvider bufferProvider = new BufferProvider(bufferSize, numBuffers, true);

        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());
        marshaller.registerSerializer(FrameableMessage.class, new FrameableSerializer());

        MessageFramer framer = new MessageFramer(marshaller);

        ServerRequestHandler handler = new ServerRequestHandler() {
            @Override
            public FrameableMessage handleRequest(FrameableMessage request) {
                RequestResponse method = requestResponseMethod.get();
                if (method != null) {
                    return method.respondTo(request);
                } else {
                    return null;
                }
            }

            @Override
            public FrameableMessage consumeSequence(long interactionId) {
                throw new UnsupportedOperationException();
            }
        };


        TcpServerInitializer initializer = new TcpServerInitializer();
        server = initializer.initialize(localHost, numWorkers, bufferProvider, framer, handler);
        server.start();


        //setup client
        bufferProvider = new BufferProvider(bufferSize, numBuffers, true);
        int connectionsPerHost = 2;
        int connectTimeoutMillis = 5000;
        int socketTimeoutMillis = 2000;
        tcpClientProvider = new TcpClientProvider(
            connectionsPerHost, connectTimeoutMillis, socketTimeoutMillis, bufferSize, bufferSize, bufferProvider, framer);

        idProvider = new IdProvider() {
            private final AtomicLong id = new AtomicLong();

            @Override
            public long nextId() {
                return id.incrementAndGet();
            }
        };
    }

    @AfterTest
    public void tearDown() throws InterruptedException {
        server.stop();
    }

    @Test(enabled = false)
    public void testMessageRoundTrip() throws Exception {
        final String sendText = "booya";
        final String returnText = "mmhmm";

        final AtomicBoolean requestReceived = new AtomicBoolean();

        requestResponseMethod.set(new RequestResponse() {
            @Override
            public FrameableMessage respondTo(FrameableMessage request) {
                if (request instanceof TestRequestAndResponse) {
                    String value = ((TestRequestAndResponse) request).getValue();
                    requestReceived.set(value.equals(sendText));
                }

                return new TestRequestAndResponse(request.getInteractionId(), returnText);
            }
        });

        TcpClient client = tcpClientProvider.getClientForHost(localHost);
        try {
            long interactionId = idProvider.nextId();
            TestRequestAndResponse request = new TestRequestAndResponse(interactionId, sendText);
            client.sendMessage(request);
            TestRequestAndResponse response = client.receiveMessage(TestRequestAndResponse.class);
            Assert.assertNotNull(response);
            Assert.assertEquals(response.getInteractionId(), interactionId);
            Assert.assertEquals(response.getValue(), returnText);
        } finally {
            tcpClientProvider.returnClient(client);
        }
    }

    public static class TestRequestAndResponse implements FrameableMessage {

        private long interactionId;
        private String value;

        public TestRequestAndResponse() {
        }

        public TestRequestAndResponse(long interactionId, String value) {
            this.interactionId = interactionId;
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public void serialize(FSTObjectOutput output) throws IOException {
            output.writeFLong(interactionId);
            output.writeStringCompressed(value);
        }

        @Override
        public void deserialize(FSTObjectInput input) throws Exception {
            this.interactionId = input.readLong();
            this.value = input.readStringCompressed();
        }

        @Override
        public long getInteractionId() {
            return interactionId;
        }

        @Override
        public boolean isLastInSequence() {
            return true;
        }
    }

    private interface RequestResponse {

        FrameableMessage respondTo(FrameableMessage request);
    }
}
