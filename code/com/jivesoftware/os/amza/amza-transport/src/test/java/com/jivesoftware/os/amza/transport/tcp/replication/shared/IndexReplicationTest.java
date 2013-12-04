package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.BinaryTimestampedValue;
import com.jivesoftware.os.amza.shared.MemoryTableIndex;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.amza.transport.tcp.replication.TcpChangeSetSender;
import com.jivesoftware.os.amza.transport.tcp.replication.TcpChangeSetTaker;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayloadSerializer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import de.ruedigermoeller.serialization.FSTConfiguration;
import java.io.IOException;
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
    private AtomicReference<TableDelta> receivedPut = new AtomicReference<>();
    private AtomicReference<TransactionSet> toTake = new AtomicReference<>();
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
        BufferProvider bufferProvider = new BufferProvider(bufferSize, numBuffers, true);

        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.createDefaultConfiguration());
        marshaller.registerSerializer(MessagePayload.class, new MessagePayloadSerializer());

        AmzaInstance amzaInstance = amzaInstance(receivedPut, toTake);
        applicationProtocol = new IndexReplicationProtocol(amzaInstance, idProvider);

        MessageFramer framer = new MessageFramer(marshaller, applicationProtocol);

        TcpServerInitializer initializer = new TcpServerInitializer();
        server = initializer.initialize(localHost, numWorkers, bufferProvider, framer, applicationProtocol);
        server.start();

        //setup client
        bufferProvider = new BufferProvider(bufferSize, numBuffers, true);
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
        TcpChangeSetSender sender = new TcpChangeSetSender(tcpClientProvider, applicationProtocol);

        TableName tableName = new TableName("test", "test", null, null);
        NavigableMap<TableIndexKey, BinaryTimestampedValue> changes = new ConcurrentSkipListMap<>();

        BinaryTimestampedValue val1 = new BinaryTimestampedValue("blah".getBytes(), idProvider.nextId(), false);
        BinaryTimestampedValue val2 = new BinaryTimestampedValue("meh".getBytes(), idProvider.nextId(), false);

        changes.put(new TableIndexKey("1".getBytes()), val1);
        changes.put(new TableIndexKey("2".getBytes()), val2);

        sender.sendChangeSet(localHost, tableName, new MemoryTableIndex(changes));

        TableDelta received = receivedPut.get();
        Assert.assertNotNull(received);
        NavigableMap<TableIndexKey, BinaryTimestampedValue> receivedApply = received.getApply();
        Assert.assertNotNull(receivedApply);
        Assert.assertEquals(receivedApply.size(), changes.size());

        Assert.assertEquals(receivedApply.get(new TableIndexKey("1".getBytes())), val1);
        Assert.assertEquals(receivedApply.get(new TableIndexKey("2".getBytes())), val2);
    }

    @Test
    public void testPull() throws Exception {
        TcpChangeSetTaker taker = new TcpChangeSetTaker(tcpClientProvider, applicationProtocol);

        TableName tableName = new TableName("test", "test", null, null);
        NavigableMap<TableIndexKey, BinaryTimestampedValue> changes = new ConcurrentSkipListMap<>();

        BinaryTimestampedValue val1 = new BinaryTimestampedValue("blah".getBytes(), idProvider.nextId(), false);
        BinaryTimestampedValue val2 = new BinaryTimestampedValue("meh".getBytes(), idProvider.nextId(), false);

        changes.put(new TableIndexKey("1".getBytes()), val1);
        changes.put(new TableIndexKey("2".getBytes()), val2);
        long transactionId = idProvider.nextId();

        TransactionSet toReturn = new TransactionSet(transactionId, changes);
        toTake.set(toReturn);

        final AtomicReference<TransactionSet> received = new AtomicReference<>();

        taker.take(localHost, tableName, transactionId, new TransactionSetStream() {
            @Override
            public boolean stream(TransactionSet transactionSet) throws Exception {
                received.set(transactionSet);
                return true;
            }
        });

        TransactionSet got = received.get();
        Assert.assertNotNull(got);
        NavigableMap<TableIndexKey, BinaryTimestampedValue> gotMap = got.getChanges();
        Assert.assertNotNull(gotMap);
        Assert.assertEquals(gotMap.size(), changes.size());

        Assert.assertEquals(gotMap.get(new TableIndexKey("1".getBytes())), val1);
        Assert.assertEquals(gotMap.get(new TableIndexKey("2".getBytes())), val2);

    }

    private AmzaInstance amzaInstance(final AtomicReference<TableDelta> put, final AtomicReference<TransactionSet> take) {
        return new AmzaInstance() {
            @Override
            public void changes(TableName tableName, TableDelta changes) throws Exception {
                put.set(changes);
            }

            @Override
            public void takeTableChanges(TableName tableName, long transationId, TransactionSetStream transactionSetStream)
                    throws Exception {
                transactionSetStream.stream(take.get());
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
        };
    }
}
