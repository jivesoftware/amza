package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.MemoryWALUpdates;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.TcpUpdatesSender;
import com.jivesoftware.os.amza.transport.tcp.replication.TcpUpdatesTaker;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.RowUpdatesPayload;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayloadSerializer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.nustaq.serialization.FSTConfiguration;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 *
 */
public class IndexReplicationTest {

    private TcpServer server;
    private RingHost localHost = new RingHost("localhost", 7766);
    private TcpClientProvider tcpClientProvider;
    private OrderIdProvider idProvider;
    private AtomicReference<Scannable<WALValue>> receivedPut = new AtomicReference<>();
    private AtomicReference<RowUpdatesPayload> toTake = new AtomicReference<>();
    private IndexReplicationProtocol applicationProtocol;
    private RowMarshaller<byte[]> rowMarshaller = new BinaryRowMarshaller();

    @BeforeTest
    public void setup() throws InterruptedException, IOException {
        idProvider = new OrderIdProvider() {
            private final AtomicLong id = new AtomicLong();

            @Override
            public long nextId() {
                return id.incrementAndGet();
            }
        };

        int bufferSize = 1024;
        int numWorkers = 10;
        int numBuffers = numWorkers * 2;

        //setup server
        BufferProvider bufferProvider = new BufferProvider(bufferSize, numBuffers, true, 1000);

        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.createDefaultConfiguration());
        marshaller.registerSerializer(MessagePayload.class, new MessagePayloadSerializer());

        AmzaInstance amzaInstance = amzaInstance(receivedPut, toTake);
        applicationProtocol = new IndexReplicationProtocol(amzaInstance, idProvider);

        MessageFramer framer = new MessageFramer(marshaller, applicationProtocol);

        TcpServerInitializer initializer = new TcpServerInitializer();
        server = initializer.initialize(localHost, numWorkers, bufferProvider, framer, applicationProtocol);
        server.start();

        //setup client
        bufferProvider = new BufferProvider(bufferSize, numBuffers, true, 1000);
        int connectionsPerHost = 2;
        int connectTimeoutMillis = 5000;
        int socketTimeoutMillis = 200000;
        tcpClientProvider = new TcpClientProvider(
            connectionsPerHost, connectTimeoutMillis, socketTimeoutMillis, bufferSize, bufferSize, bufferProvider, framer);
    }

    @AfterTest
    public void tearDown() throws InterruptedException {
        if (server != null) {
            server.stop();
        }
    }

    @Test(enabled = false)
    public void testPush() throws Exception {
        TcpUpdatesSender sender = new TcpUpdatesSender(tcpClientProvider, applicationProtocol);

        RegionName tableName = new RegionName(false, "test", "test");
        Map<WALKey, WALValue> changes = new HashMap<>();

        WALValue val1 = new WALValue("blah".getBytes(), idProvider.nextId(), false);
        WALValue val2 = new WALValue("meh".getBytes(), idProvider.nextId(), false);

        changes.put(new WALKey("1".getBytes()), val1);
        changes.put(new WALKey("2".getBytes()), val2);

        sender.sendUpdates(localHost, tableName, new MemoryWALUpdates(changes));

        Scannable<WALValue> received = receivedPut.get();
        Assert.assertNotNull(received);
        final NavigableMap<WALKey, WALValue> receivedApply = new ConcurrentSkipListMap<>();
        received.rowScan((long orderId, WALKey key, WALValue value) -> {
            receivedApply.put(key, value);
            return true;
        });
        Assert.assertNotNull(receivedApply);
        Assert.assertEquals(receivedApply.size(), changes.size());

        Assert.assertEquals(receivedApply.get(new WALKey("1".getBytes())), val1);
        Assert.assertEquals(receivedApply.get(new WALKey("2".getBytes())), val2);
    }

    @Test(enabled = false)
    public void testPull() throws Exception {
        TcpUpdatesTaker taker = new TcpUpdatesTaker(tcpClientProvider, applicationProtocol);

        RegionName tableName = new RegionName(false, "test", "test");
        NavigableMap<WALKey, WALValue> changes = new ConcurrentSkipListMap<>();

        WALValue val1 = new WALValue("blah".getBytes(), idProvider.nextId(), false);
        WALValue val2 = new WALValue("meh".getBytes(), idProvider.nextId(), false);

        changes.put(new WALKey("1".getBytes()), val1);
        changes.put(new WALKey("2".getBytes()), val2);
        long transactionId = idProvider.nextId();

        toTake.set(new RowUpdatesPayload(tableName, transactionId, Arrays.asList(new WALKey("1".getBytes()), new WALKey("2".getBytes())),
            Arrays.asList(val1, val2)));

        final NavigableMap<WALKey, WALValue> received = new ConcurrentSkipListMap<>();
// TODO fix
//        taker.takeUpdates(localHost, tableName, transactionId, new WALScan() {
//
//            @Override
//            public boolean row(long orderId, WALKey key, WALValue value) throws Exception {
//                received.put(key, value);
//                return true;
//            }
//        });

        Assert.assertNotNull(received);
        Assert.assertEquals(received.size(), changes.size());

        Assert.assertEquals(received.get(new WALKey("1".getBytes())), val1);
        Assert.assertEquals(received.get(new WALKey("2".getBytes())), val2);

    }

    private AmzaInstance amzaInstance(final AtomicReference<Scannable<WALValue>> put, final AtomicReference<RowUpdatesPayload> take) {
        return new AmzaInstance() {
            @Override
            public void updates(RegionName tableName, Scannable<WALValue> changes) throws Exception {
                put.set(changes);
            }

            @Override
            public void takeRowUpdates(RegionName tableName, long transationId, RowStream rowStream)
                throws Exception {
                take.get().rowScan(rowStream);
            }

            @Override
            public List<RegionName> getRegionNames() {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void destroyRegion(RegionName tableName) throws Exception {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public long getTimestamp(long timestamp, long millisAgo) throws Exception {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        };
    }
}
