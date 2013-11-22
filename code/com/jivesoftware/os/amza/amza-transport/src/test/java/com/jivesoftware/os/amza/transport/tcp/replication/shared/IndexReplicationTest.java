package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.BasicTimestampedValue;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TimestampedValue;
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
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import junit.framework.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 *
 */
public class IndexReplicationTest {

    private TcpServer server;
    private RingHost localHost = new RingHost("localhost", 7777);
    private TcpClientProvider tcpClientProvider;
    private OrderIdProvider idProvider;
    private int requestOpcode = 45;
    private int responseOpcode = 47;
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

        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());
        marshaller.registerSerializer(MessagePayload.class, new MessagePayloadSerializer());

        final Map<Integer, Class<? extends Serializable>> payloadRegistry = new HashMap<>();
        payloadRegistry.put(requestOpcode, String.class);
        payloadRegistry.put(responseOpcode, String.class);

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
        server.stop();
    }

    @Test(enabled = false) //fst is sad when both test methods run
    public void testPush() throws Exception {
        TcpChangeSetSender sender = new TcpChangeSetSender(tcpClientProvider, applicationProtocol);

        TableName tableName = new TableName("test", "test", String.class, "1", "3", String.class);
        NavigableMap<String, TimestampedValue<String>> changes = new ConcurrentSkipListMap<>();

        TimestampedValue<String> val1 = new BasicTimestampedValue<>("blah", idProvider.nextId(), false);
        TimestampedValue<String> val2 = new BasicTimestampedValue<>("meh", idProvider.nextId(), false);

        changes.put("1", val1);
        changes.put("2", val2);

        sender.sendChangeSet(localHost, tableName, changes);

        TableDelta received = receivedPut.get();
        Assert.assertNotNull(received);
        NavigableMap<String, TimestampedValue<String>> receivedApply = received.getApply();
        Assert.assertNotNull(receivedApply);
        Assert.assertEquals(receivedApply.size(), changes.size());

        Assert.assertEquals(receivedApply.get("1"), val1);
        Assert.assertEquals(receivedApply.get("2"), val2);
    }

    @Test
    public void testPull() throws Exception {
        TcpChangeSetTaker taker = new TcpChangeSetTaker(tcpClientProvider, applicationProtocol);

        TableName tableName = new TableName("test", "test", String.class, "1", "3", String.class);
        NavigableMap<String, TimestampedValue<String>> changes = new ConcurrentSkipListMap<>();

        TimestampedValue<String> val1 = new BasicTimestampedValue<>("blah", idProvider.nextId(), false);
        TimestampedValue<String> val2 = new BasicTimestampedValue<>("meh", idProvider.nextId(), false);

        changes.put("1", val1);
        changes.put("2", val2);
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
        NavigableMap<String, TimestampedValue<String>> gotMap = got.getChanges();
        Assert.assertNotNull(gotMap);
        Assert.assertEquals(gotMap.size(), changes.size());

        Assert.assertEquals(gotMap.get("1"), val1);
        Assert.assertEquals(gotMap.get("2"), val2);

    }

    private AmzaInstance amzaInstance(final AtomicReference<TableDelta> put, final AtomicReference<TransactionSet> take) {
        return new AmzaInstance() {
            @Override
            public <K, V> void changes(TableName<K, V> tableName, TableDelta<K, V> changes) throws Exception {
                put.set(changes);
            }

            @Override
            public <K, V> void takeTableChanges(TableName<K, V> tableName, long transationId, TransactionSetStream<K, V> transactionSetStream)
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
        };
    }
}
