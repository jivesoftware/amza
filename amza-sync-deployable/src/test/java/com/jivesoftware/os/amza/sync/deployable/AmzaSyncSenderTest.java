package com.jivesoftware.os.amza.sync.deployable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.RingPartitionProperties;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream.TxResult;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.amza.client.test.InMemoryPartitionClient;
import com.jivesoftware.os.amza.sync.api.AmzaSyncPartitionConfig;
import com.jivesoftware.os.amza.sync.api.AmzaSyncPartitionTuple;
import com.jivesoftware.os.amza.sync.api.AmzaSyncSenderConfig;
import com.jivesoftware.os.aquarium.AquariumStats;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class AmzaSyncSenderTest {

    @Test
    public void testProgress() throws Exception {
        byte[] partitionBytes = "partition1".getBytes(StandardCharsets.UTF_8);
        PartitionName partitionName = new PartitionName(false, partitionBytes, partitionBytes);
        RingMember ringMember = new RingMember("member1");

        TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1),
            new SnowflakeIdPacker(),
            new JiveEpochTimestampProvider());
        PartitionClientProvider partitionClientProvider = new InMemoryPartitionClientProvider(orderIdProvider, ringMember);

        PartitionClient partition = partitionClientProvider.getPartition(partitionName);

        AmzaClientAquariumProvider amzaClientAquariumProvider = new AmzaClientAquariumProvider(new AquariumStats(),
            "test",
            partitionClientProvider,
            orderIdProvider,
            ringMember.asAquariumMember(),
            count -> count == 1,
            () -> Sets.newHashSet(ringMember.asAquariumMember()),
            128,
            128,
            5_000L,
            100L,
            60_000L,
            10_000L,
            Executors.newSingleThreadExecutor(),
            100L,
            1_000L,
            10_000L,
            false);

        int[] rowCount = new int[1];
        AmzaSyncClient syncClient = new AmzaSyncClient() {

            @Override
            public void commitRows(PartitionName toPartitionName, List<Row> rows) throws Exception {
                rowCount[0] += rows.size();
            }

            @Override
            public void ensurePartition(PartitionName toPartitionName, PartitionProperties properties, int ringSize) throws Exception {
            }
        };

        AmzaSyncSender syncSender = new AmzaSyncSender(
            new AmzaSyncStats(),
            new AmzaSyncSenderConfig("default",
                true,
                100L,
                1_000,
                false,
                "",
                "",
                -1,
                "",
                "",
                "",
                true),
            amzaClientAquariumProvider,
            1,
            Executors.newScheduledThreadPool(1),
            partitionClientProvider,
            syncClient,
            (name) -> ImmutableMap.of(new AmzaSyncPartitionTuple(partitionName, partitionName),
                new AmzaSyncPartitionConfig(0, 0, 0, 0, 0)),
            new AmzaInterner());

        amzaClientAquariumProvider.start();
        syncSender.start();

        AtomicInteger keyProvider = new AtomicInteger();
        AtomicLong valueProvider = new AtomicLong();
        AtomicLong largestTxId = new AtomicLong();
        long initialTimeMillis = System.currentTimeMillis();

        largestTxId.set(advancePartition(partition, 10, initialTimeMillis, keyProvider, valueProvider));
        Assert.assertTrue(largestTxId.get() > 0, "Expected positive txId");

        long failAfter = System.currentTimeMillis() + 60_000L;
        Cursor cursor = awaitCursor(partitionName, syncSender, ringMember, largestTxId.get(), failAfter);

        Assert.assertNotNull(cursor);
        Assert.assertFalse(cursor.taking);
        Assert.assertEquals(initialTimeMillis, cursor.maxTimestamp);
        Assert.assertNotNull(cursor.memberTxIds);
        Assert.assertTrue(cursor.memberTxIds.containsKey(ringMember));
        Assert.assertEquals(cursor.memberTxIds.get(ringMember).longValue(), 10L);
        Assert.assertEquals(rowCount[0], 10);

        rowCount[0] = 0;
        long nextTimeMillis = initialTimeMillis + 1;
        largestTxId.set(advancePartition(partition, 10, nextTimeMillis, keyProvider, valueProvider));
        Assert.assertTrue(largestTxId.get() > 0, "Expected positive txId");
        cursor = awaitCursor(partitionName, syncSender, ringMember, largestTxId.get(), failAfter);

        Assert.assertNotNull(cursor);
        Assert.assertFalse(cursor.taking);
        Assert.assertEquals(nextTimeMillis, cursor.maxTimestamp);
        Assert.assertNotNull(cursor.memberTxIds);
        Assert.assertTrue(cursor.memberTxIds.containsKey(ringMember));
        Assert.assertEquals(cursor.memberTxIds.get(ringMember).longValue(), 20L);
        Assert.assertEquals(rowCount[0], 10);
    }

    private long advancePartition(PartitionClient partition,
        int rows,
        long currentTimeMillis,
        AtomicInteger keyProvider,
        AtomicLong valueProvider) throws Exception {

        partition.commit(Consistency.none,
            null,
            commitKeyValueStream -> {
                for (int i = 0; i < rows; i++) {
                    commitKeyValueStream.commit(UIO.intBytes(keyProvider.incrementAndGet()),
                        UIO.longBytes(valueProvider.incrementAndGet()),
                        currentTimeMillis,
                        false);
                }
                return true;
            },
            1_000L,
            10_000L,
            Optional.empty());

        long[] txId = { -1 };
        partition.takeFromTransactionId(null, null, -1,
            highwater -> {
            },
            (rowTxId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                txId[0] = rowTxId;
                return TxResult.MORE;
            },
            1_000L,
            10_000L,
            Optional.empty());
        return txId[0];
    }

    private Cursor awaitCursor(PartitionName partitionName,
        AmzaSyncSender syncSender,
        RingMember awaitRingMember,
        long awaitTransactionId,
        long failAfter) throws Exception {
        Cursor[] cursor = new Cursor[1];
        while (true) {
            syncSender.streamCursors(partitionName, partitionName, (fromPartitionName, toPartitionName, timestamp, cursor1) -> {
                Assert.assertEquals(toPartitionName, partitionName);
                cursor[0] = cursor1;
                return true;
            });
            Long gotTransactionId = cursor[0] == null ? null : cursor[0].memberTxIds.get(awaitRingMember);
            if (gotTransactionId != null && gotTransactionId >= awaitTransactionId) {
                break;
            }
            if (System.currentTimeMillis() > failAfter) {
                Assert.fail("Timed out awaiting progress");
            }
            Thread.sleep(100L);
        }
        return cursor[0];
    }

    private static class InMemoryPartitionClientProvider implements PartitionClientProvider {

        private final OrderIdProvider orderIdProvider;
        private final RingMember ringMember;

        private final Map<PartitionName, PartitionClient> clients = Maps.newConcurrentMap();

        public InMemoryPartitionClientProvider(OrderIdProvider orderIdProvider, RingMember ringMember) {
            this.orderIdProvider = orderIdProvider;
            this.ringMember = ringMember;
        }

        @Override
        public RingPartitionProperties getProperties(PartitionName partitionName) throws Exception {
            return new RingPartitionProperties(1,
                new PartitionProperties(Durability.fsync_never, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, false, Consistency.none, false, true, false,
                    RowType.primary, "lab", 8, null, 4096, 16));
        }

        @Override
        public PartitionClient getPartition(PartitionName partitionName) throws Exception {
            return clients.computeIfAbsent(partitionName,
                partitionName1 -> new InMemoryPartitionClient(ringMember,
                    new ConcurrentSkipListMap<>(),
                    new ConcurrentSkipListMap<>(KeyUtil.lexicographicalComparator()),
                    orderIdProvider));
        }

        @Override
        public PartitionClient getPartition(PartitionName partitionName, int ringSize, PartitionProperties partitionProperties) throws Exception {
            return getPartition(partitionName);
        }
    }
}
