package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.MemoryRowsIndex;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.RowScanable;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.transport.tcp.replication.TcpUpdatesSender;
import com.jivesoftware.os.amza.transport.tcp.replication.TcpUpdatesTaker;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.RowUpdatesPayload;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayloadSerializer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import de.ruedigermoeller.serialization.FSTConfiguration;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
    private AtomicReference<RowScanable> receivedPut = new AtomicReference<>();
    private AtomicReference<RowUpdatesPayload> toTake = new AtomicReference<>();
    private IndexReplicationProtocol applicationProtocol;

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

    @Test
    public void testPush() throws Exception {
        TcpUpdatesSender sender = new TcpUpdatesSender(tcpClientProvider, applicationProtocol);

        TableName tableName = new TableName("test", "test", null, null);
        NavigableMap<RowIndexKey, RowIndexValue> changes = new ConcurrentSkipListMap<>();

        RowIndexValue val1 = new RowIndexValue("blah".getBytes(), idProvider.nextId(), false);
        RowIndexValue val2 = new RowIndexValue("meh".getBytes(), idProvider.nextId(), false);

        changes.put(new RowIndexKey("1".getBytes()), val1);
        changes.put(new RowIndexKey("2".getBytes()), val2);

        sender.sendUpdates(localHost, tableName, new MemoryRowsIndex(changes));

        RowScanable received = receivedPut.get();
        Assert.assertNotNull(received);
        final NavigableMap<RowIndexKey, RowIndexValue> receivedApply = new ConcurrentSkipListMap<>();
        received.rowScan(new RowScan<RuntimeException>() {

            @Override
            public boolean row(long orderId, RowIndexKey key, RowIndexValue value) throws RuntimeException {
                receivedApply.put(key, value);
                return true;
            }
        });
        Assert.assertNotNull(receivedApply);
        Assert.assertEquals(receivedApply.size(), changes.size());

        Assert.assertEquals(receivedApply.get(new RowIndexKey("1".getBytes())), val1);
        Assert.assertEquals(receivedApply.get(new RowIndexKey("2".getBytes())), val2);
    }

    @Test
    public void testPull() throws Exception {
        TcpUpdatesTaker taker = new TcpUpdatesTaker(tcpClientProvider, applicationProtocol);

        TableName tableName = new TableName("test", "test", null, null);
        NavigableMap<RowIndexKey, RowIndexValue> changes = new ConcurrentSkipListMap<>();

        RowIndexValue val1 = new RowIndexValue("blah".getBytes(), idProvider.nextId(), false);
        RowIndexValue val2 = new RowIndexValue("meh".getBytes(), idProvider.nextId(), false);

        changes.put(new RowIndexKey("1".getBytes()), val1);
        changes.put(new RowIndexKey("2".getBytes()), val2);
        long transactionId = idProvider.nextId();

        toTake.set(new RowUpdatesPayload(tableName, transactionId, Arrays.asList(new RowIndexKey("1".getBytes()), new RowIndexKey("2".getBytes())),
                Arrays.asList(val1, val2)));

        final NavigableMap<RowIndexKey, RowIndexValue> received = new ConcurrentSkipListMap<>();
        taker.takeUpdates(localHost, tableName, transactionId, new RowScan() {

            @Override
            public boolean row(long orderId, RowIndexKey key, RowIndexValue value) throws Exception {
                received.put(key, value);
                return true;
            }
        });

        Assert.assertNotNull(received);
        Assert.assertEquals(received.size(), changes.size());

        Assert.assertEquals(received.get(new RowIndexKey("1".getBytes())), val1);
        Assert.assertEquals(received.get(new RowIndexKey("2".getBytes())), val2);

    }

    private AmzaInstance amzaInstance(final AtomicReference<RowScanable> put, final AtomicReference<RowUpdatesPayload> take) {
        return new AmzaInstance() {
            @Override
            public void updates(TableName tableName, RowScanable changes) throws Exception {
                put.set(changes);
            }

            @Override
            public void takeRowUpdates(TableName tableName, long transationId, RowScan rowStream)
                    throws Exception {
                take.get().rowScan(rowStream);
            }

            @Override
            public void addRingHost(String ringName, RingHost ringHost) throws Exception {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void removeRingHost(String ringName, RingHost ringHost) throws Exception {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public List<RingHost> getRing(String ringName) throws Exception {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public List<TableName> getTableNames() {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void destroyTable(TableName tableName) throws Exception {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public long getTimestamp(long timestamp, long millisAgo) throws Exception {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        };
    }
}
