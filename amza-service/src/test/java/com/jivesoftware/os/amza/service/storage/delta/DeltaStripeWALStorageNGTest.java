package com.jivesoftware.os.amza.service.storage.delta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.StorageVersion;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.KeyContainedStream;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.UnprefixedTxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.api.wal.WALValue;
import com.jivesoftware.os.amza.service.AckWaters;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.IndexedWALStorageProvider;
import com.jivesoftware.os.amza.service.SickPartitions;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.filer.HeapByteBufferFactory;
import com.jivesoftware.os.amza.service.replication.CurrentVersionProvider;
import com.jivesoftware.os.amza.service.replication.PartitionBackedHighwaterStorage;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.stats.IoStats;
import com.jivesoftware.os.amza.service.storage.JacksonPartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.MemoryBackedRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.jive.utils.collections.bah.ConcurrentBAHash;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class DeltaStripeWALStorageNGTest {

    private final WALUpdated updated = (versionedPartitionName, txId) -> {
    };

    private final RowChanges rowChanges = changes -> {
    };

    private final VersionedPartitionName versionedPartitionName1 = new VersionedPartitionName(
        new PartitionName(false, "ring1".getBytes(), "partitionName1".getBytes()),
        VersionedPartitionName.STATIC_VERSION);
    private final VersionedPartitionName versionedPartitionName2 = new VersionedPartitionName(
        new PartitionName(false, "ring2".getBytes(), "partitionName2".getBytes()),
        VersionedPartitionName.STATIC_VERSION);

    private final BAInterner interner = new BAInterner();
    private final BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller();
    private final BinaryHighwaterRowMarshaller highwaterRowMarshaller = new BinaryHighwaterRowMarshaller(interner);

    private JacksonPartitionPropertyMarshaller partitionPropertyMarshaller;
    private IndexedWALStorageProvider indexedWALStorageProvider;
    private TimestampedOrderIdProvider orderIdProvider;
    private AmzaStats amzaStats;
    private SystemWALStorage systemWALStorage;
    private PartitionCreator partitionCreator;
    private PartitionIndex partitionIndex;
    private CurrentVersionProvider currentVersionProvider;
    private PartitionStore partitionStore1;
    private PartitionStore partitionStore2;
    private DeltaWALFactory deltaWALFactory;
    private WALIndexProviderRegistry walIndexProviderRegistry;
    private DeltaStripeWALStorage deltaStripeWALStorage;
    private AmzaRingStoreReader ringStoreReader;
    private HighwaterStorage highwaterStorage;
    private RowType testRowType1 = RowType.snappy_primary;
    private RowType testRowType2 = RowType.primary;

    @BeforeMethod
    public void setup() throws Exception {
        RingMember member = new RingMember("localhost");
        OrderIdProviderImpl ids = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        ObjectMapper mapper = new ObjectMapper();
        partitionPropertyMarshaller = new JacksonPartitionPropertyMarshaller(mapper);

        File partitionTmpDir = Files.createTempDir();
        File[] workingDirectories = { partitionTmpDir };
        IoStats ioStats = new IoStats();
        MemoryBackedRowIOProvider ephemeralRowIOProvider = new MemoryBackedRowIOProvider(
            ioStats,
            1_024,
            1_024 * 1_024,
            4_096,
            64,
            new HeapByteBufferFactory());
        BinaryRowIOProvider persistentRowIOProvider = new BinaryRowIOProvider(
            ioStats,
            4_096,
            64,
            false);
        walIndexProviderRegistry = new WALIndexProviderRegistry(ephemeralRowIOProvider, persistentRowIOProvider);

        amzaStats = new AmzaStats();
        indexedWALStorageProvider = new IndexedWALStorageProvider(amzaStats,
            workingDirectories,
            workingDirectories.length,
            walIndexProviderRegistry,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            ids,
            new SickPartitions(),
            -1,
            TimeUnit.DAYS.toMillis(1));

        orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1), new SnowflakeIdPacker(),
            new JiveEpochTimestampProvider());
        partitionIndex = new PartitionIndex(amzaStats,
            orderIdProvider,
            indexedWALStorageProvider,
            4);

        currentVersionProvider = new CurrentVersionProvider() {
            @Override
            public boolean isCurrentVersion(VersionedPartitionName versionedPartitionName) {
                return versionedPartitionName.getPartitionVersion() == VersionedPartitionName.STATIC_VERSION;
            }

            @Override
            public void abandonVersion(VersionedPartitionName versionedPartitionName) throws Exception {
                throw new UnsupportedOperationException("Stop it");
            }

            @Override
            public <R> R tx(PartitionName partitionName, StorageVersion storageVersion, StripeIndexs<R> tx) throws Exception {
                return tx.tx(0, 0, new StorageVersion(0, 0));
            }

            @Override
            public void invalidateDeltaIndexCache(VersionedPartitionName versionedPartitionName) throws Exception {
            }
        };

        systemWALStorage = new SystemWALStorage(partitionIndex,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            null,
            false);

        partitionCreator = new PartitionCreator(ids,
            partitionPropertyMarshaller,
            partitionIndex,
            systemWALStorage,
            updated,
            rowChanges,
            interner);

        partitionCreator.init((partitionName) -> 0);

        PartitionProperties properties1 = new PartitionProperties(Durability.fsync_never,
            0, 0, 0, 0, 0, 0, 0, 0,
            false,
            Consistency.none,
            true,
            false,
            false,
            testRowType1,
            "memory_persistent",
            -1,
            null,
            -1,
            -1);
        partitionCreator.createPartitionIfAbsent(versionedPartitionName1.getPartitionName(), properties1);

        PartitionProperties properties2 = new PartitionProperties(Durability.fsync_never,
            0, 0, 0, 0, 0, 0, 0, 0,
            false,
            Consistency.none,
            true,
            false,
            false,
            testRowType2,
            "memory_persistent",
            -1,
            null,
            -1,
            -1);
        partitionCreator.createPartitionIfAbsent(versionedPartitionName2.getPartitionName(), properties2);

        partitionStore1 = partitionIndex.get(versionedPartitionName1, properties1, 0);
        partitionStore2 = partitionIndex.get(versionedPartitionName2, properties2, 0);
        Assert.assertNotNull(partitionStore1);
        Assert.assertNotNull(partitionStore2);

        highwaterStorage = new PartitionBackedHighwaterStorage(interner, ids, member, partitionCreator, systemWALStorage, updated, 100);

        File tmp = Files.createTempDir();
        workingDirectories = new File[] { tmp };
        RowIOProvider ioProvider = new BinaryRowIOProvider(ioStats, 4_096, 64, false);
        deltaWALFactory = new DeltaWALFactory(ids, tmp, ioProvider, primaryRowMarshaller, highwaterRowMarshaller, 100);
        deltaStripeWALStorage = loadDeltaStripe();

        ringStoreReader = new AmzaRingStoreReader(interner,
            member,
            new ConcurrentBAHash<>(13, true, 4),
            new ConcurrentBAHash<>(13, true, 4),
            new AtomicLong(),
            ImmutableSet.of());
        ringStoreReader.start(partitionIndex);
    }

    private DeltaStripeWALStorage loadDeltaStripe() throws Exception {

        DeltaStripeWALStorage delta = new DeltaStripeWALStorage(interner,
            1,
            new AmzaStats(),
            new AckWaters(amzaStats, 2),
            new SickThreads(),
            ringStoreReader,
            deltaWALFactory,
            walIndexProviderRegistry,
            20_000, 8);
        delta.load(partitionIndex, partitionCreator, currentVersionProvider, primaryRowMarshaller);
        return delta;
    }

    @Test
    public void test() throws Exception {
        WALStorage storage1 = partitionStore1.getWalStorage();
        WALStorage storage2 = partitionStore2.getWalStorage();
        byte[] prefix = UIO.intBytes(-1);
        WALKey walKey = key(prefix, 1);

        Assert.assertNull(deltaStripeWALStorage.get(versionedPartitionName1, storage1, walKey.prefix, walKey.key));
        deltaStripeWALStorage.containsKeys(versionedPartitionName1, storage1, prefix, keys(1), assertKeyIsContained(false));
        Assert.assertEquals(storage1.count(keyStream -> true), 0);
        Assert.assertNull(storage1.getTimestampedValue(walKey.prefix, walKey.key));

        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefix,
            new IntUpdate(testRowType1, 1, 1, 2, false),
            updated);

        deltaStripeWALStorage.get(versionedPartitionName1, storage1, prefix, keyStream -> keyStream.stream(walKey.key),
            (_prefix, key, value, timestamp, tombstoned, version) -> {
                Assert.assertFalse(tombstoned);
                Assert.assertEquals(key, walKey.key);
                Assert.assertEquals(value, UIO.intBytes(1));
                return true;
            });

        Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName1, storage1, walKey.prefix, walKey.key),
            new WALValue(null, UIO.intBytes(1), 2, false, 2));
        deltaStripeWALStorage.containsKeys(versionedPartitionName1, storage1, prefix, keys(1), assertKeyIsContained(true));
        Assert.assertEquals(storage1.count(keyStream -> true), 0);
        Assert.assertNull(storage1.getTimestampedValue(walKey.prefix, walKey.key));

        deltaStripeWALStorage.merge(partitionIndex, partitionCreator, currentVersionProvider, true);

        Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName1, storage1, walKey.prefix, walKey.key),
            new WALValue(null, UIO.intBytes(1), 2, false, 2));
        deltaStripeWALStorage.containsKeys(versionedPartitionName1, storage1, prefix, keys(1), assertKeyIsContained(true));
        Assert.assertEquals(storage1.getTimestampedValue(walKey.prefix, walKey.key), new TimestampedValue(2, 2, UIO.intBytes(1)));
        Assert.assertEquals(storage1.count(keyStream -> true), 1);

        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefix,
            new IntUpdate(testRowType1, 1, 1, 1, false),
            updated);
        Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName1, storage1, walKey.prefix, walKey.key),
            new WALValue(null, UIO.intBytes(1), 2, false, 2));
        deltaStripeWALStorage.containsKeys(versionedPartitionName1, storage1, prefix, keys(1), assertKeyIsContained(true));
        Assert.assertEquals(storage1.getTimestampedValue(walKey.prefix, walKey.key), new TimestampedValue(2, 2, UIO.intBytes(1)));
        Assert.assertEquals(storage1.count(keyStream -> true), 1);

        deltaStripeWALStorage.merge(partitionIndex, partitionCreator, currentVersionProvider, true);

        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefix,
            new IntUpdate(testRowType1, 1, 1, 3, true),
            updated);
        Assert.assertTrue(deltaStripeWALStorage.get(versionedPartitionName1, storage1, walKey.prefix, walKey.key).getTombstoned());
        deltaStripeWALStorage.containsKeys(versionedPartitionName1, storage1, prefix, keys(1), assertKeyIsContained(false));
        Assert.assertEquals(storage1.getTimestampedValue(walKey.prefix, walKey.key), new TimestampedValue(2, 2, UIO.intBytes(1)));
        Assert.assertEquals(storage1.count(keyStream -> true), 1);

        deltaStripeWALStorage.merge(partitionIndex, partitionCreator, currentVersionProvider, true);
        Assert.assertTrue(deltaStripeWALStorage.get(versionedPartitionName1, storage1, walKey.prefix, walKey.key).getTombstoned());
        deltaStripeWALStorage.containsKeys(versionedPartitionName1, storage1, prefix, keys(1), assertKeyIsContained(false));
        Assert.assertNull(storage1.getTimestampedValue(walKey.prefix, walKey.key));
        Assert.assertEquals(storage1.count(keyStream -> true), 1);

        File baseKey = indexedWALStorageProvider.baseKey(versionedPartitionName1, 0);
        storage1.compactTombstone(baseKey, baseKey, testRowType1, 10, 10, Long.MAX_VALUE, Long.MAX_VALUE, -1, 0, true,
            (transitionToCompacted) -> transitionToCompacted.tx(() -> {
                return null;
            }));
        storage1.compactTombstone(baseKey, baseKey, testRowType1, 10, 10, Long.MAX_VALUE, Long.MAX_VALUE, -1, 0, true,
            (transitionToCompacted) -> transitionToCompacted.tx(() -> {
                return null;
            })); // Bla

        Assert.assertEquals(storage1.count(keyStream -> true), 0);

        for (int i = 2; i <= 10; i++) {
            deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefix,
                new IntUpdate(testRowType1, i, i, 4, false),
                updated);
            deltaStripeWALStorage.update(true, testRowType2, highwaterStorage, versionedPartitionName2, partitionStore2, prefix,
                new IntUpdate(testRowType2, i, i, 4, false),
                updated);
        }

        for (int i = 2; i <= 10; i++) {
            WALKey addWalKey = key(prefix, i);
            Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName1, storage1, addWalKey.prefix, addWalKey.key),
                new WALValue(null, UIO.intBytes(i), 4, false, 4));
            Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName2, storage2, addWalKey.prefix, addWalKey.key),
                new WALValue(null, UIO.intBytes(i), 4, false, 4));
        }

        deltaStripeWALStorage.hackTruncation(4);

        partitionIndex = new PartitionIndex(amzaStats, orderIdProvider, indexedWALStorageProvider, 4);
        partitionCreator = new PartitionCreator(orderIdProvider, partitionPropertyMarshaller, partitionIndex, systemWALStorage, updated, rowChanges, interner);
        partitionCreator.init((partitionName) -> 0);
        deltaStripeWALStorage = loadDeltaStripe();

        for (int i = 2; i <= 10; i++) {
            WALKey addWalKey = key(prefix, i);
            Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName1, storage1, addWalKey.prefix, addWalKey.key),
                new WALValue(null, UIO.intBytes(i), 4, false, 4));
            if (i == 10) {
                Assert.assertNull(deltaStripeWALStorage.get(versionedPartitionName2, storage2, addWalKey.prefix, addWalKey.key));
            } else {
                Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName2, storage2, addWalKey.prefix, addWalKey.key),
                    new WALValue(null, UIO.intBytes(i), 4, false, 4));
            }
        }
    }

    @Test
    public void testTombstones() throws Exception {
        WALStorage storage = partitionStore1.getWalStorage();
        byte[] prefix = UIO.intBytes(-1);
        byte[] key1 = UIO.intBytes(1);
        byte[] key2 = UIO.intBytes(2);

        Assert.assertNull(deltaStripeWALStorage.get(versionedPartitionName1, storage, prefix, key1));
        Assert.assertNull(deltaStripeWALStorage.get(versionedPartitionName1, storage, prefix, key2));
        deltaStripeWALStorage.containsKeys(versionedPartitionName1, storage, prefix, keys(1), assertKeyIsContained(false));
        deltaStripeWALStorage.containsKeys(versionedPartitionName1, storage, prefix, keys(2), assertKeyIsContained(false));
        Assert.assertNull(storage.getTimestampedValue(prefix, key1));
        Assert.assertNull(storage.getTimestampedValue(prefix, key2));

        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefix,
            new IntUpdate(testRowType1, 1, 1, 1, false),
            updated);
        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefix,
            new IntUpdate(testRowType1, 2, 2, 1, false),
            updated);

        Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName1, storage, prefix, key1),
            new WALValue(null, UIO.intBytes(1), 1, false, 1));
        Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName1, storage, prefix, key2),
            new WALValue(null, UIO.intBytes(2), 1, false, 1));
        deltaStripeWALStorage.containsKeys(versionedPartitionName1, storage, prefix, keys(1), assertKeyIsContained(true));
        deltaStripeWALStorage.containsKeys(versionedPartitionName1, storage, prefix, keys(2), assertKeyIsContained(true));

        deltaStripeWALStorage.get(versionedPartitionName1, storage, prefix, keys(1), (_prefix, key, value, timestamp, tombstoned, version) -> {
            Assert.assertFalse(tombstoned);
            Assert.assertEquals(key, key1);
            Assert.assertEquals(value, UIO.intBytes(1));
            return true;
        });
        deltaStripeWALStorage.get(versionedPartitionName1, storage, prefix, keys(2), (_prefix, key, value, timestamp, tombstoned, version) -> {
            Assert.assertFalse(tombstoned);
            Assert.assertEquals(key, key2);
            Assert.assertEquals(value, UIO.intBytes(2));
            return true;
        });

        deltaStripeWALStorage.merge(partitionIndex, partitionCreator, currentVersionProvider, false);

        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefix,
            new IntUpdate(testRowType1, 1, 1, 2, true),
            updated);
        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefix,
            new IntUpdate(testRowType1, 2, 2, 2, true),
            updated);

        Assert.assertTrue(deltaStripeWALStorage.get(versionedPartitionName1, storage, prefix, key1).getTombstoned());
        Assert.assertTrue(deltaStripeWALStorage.get(versionedPartitionName1, storage, prefix, key2).getTombstoned());
        deltaStripeWALStorage.containsKeys(versionedPartitionName1, storage, prefix, keys(1), assertKeyIsContained(false));
        deltaStripeWALStorage.containsKeys(versionedPartitionName1, storage, prefix, keys(2), assertKeyIsContained(false));

        deltaStripeWALStorage.get(versionedPartitionName1, storage, prefix, keys(1), (_prefix, key, value, timestamp, tombstoned, version) -> {
            Assert.assertTrue(tombstoned);
            Assert.assertEquals(key, key1);
            Assert.assertNull(value);
            Assert.assertEquals(timestamp, 2);
            return true;
        });
        deltaStripeWALStorage.get(versionedPartitionName1, storage, prefix, keys(2), (_prefix, key, value, timestamp, tombstoned, version) -> {
            Assert.assertTrue(tombstoned);
            Assert.assertEquals(key, key2);
            Assert.assertNull(value);
            Assert.assertEquals(timestamp, 2);
            return true;
        });
    }

    @Test
    public void testTakeWithPrefix() throws Exception {
        WALStorage storage = partitionStore1.getWalStorage();
        byte[] prefixA = "a".getBytes();
        byte[] prefixB = "b".getBytes();

        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefixA,
            new IntUpdate(testRowType1, 1, 101, 1001, false),
            updated);
        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefixB,
            new IntUpdate(testRowType1, 1, 201, 2001, false),
            updated);
        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefixA,
            new IntUpdate(testRowType1, 2, 102, 1002, false),
            updated);
        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefixB,
            new IntUpdate(testRowType1, 2, 202, 2002, false),
            updated);
        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefixA,
            new IntUpdate(testRowType1, 3, 103, 1003, false),
            updated);
        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefixB,
            new IntUpdate(testRowType1, 3, 203, 2003, false),
            updated);
        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefixA,
            new IntUpdate(testRowType1, 4, 104, 1004, false),
            updated);
        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, prefixB,
            new IntUpdate(testRowType1, 4, 204, 2004, false),
            updated);

        int[] index = new int[1];
        deltaStripeWALStorage.takeRowsFromTransactionId(versionedPartitionName1, storage, prefixA, 0, (rowFP, rowTxId, rowType, row) -> {
            if (rowType == testRowType1) {
                index[0]++;
                byte[] value = primaryRowMarshaller.valueFromRow(rowType, row, 0);
                long timestamp = primaryRowMarshaller.timestampFromRow(row, 0);
                Assert.assertEquals(value, UIO.intBytes(100 + index[0]));
                Assert.assertEquals(timestamp, 1000 + index[0]);
            }
            return true;
        });
        Assert.assertEquals(index[0], 4);

        index[0] = 0;
        deltaStripeWALStorage.takeRowsFromTransactionId(versionedPartitionName1, storage, prefixB, 0, (rowFP, rowTxId, rowType, row) -> {
            if (rowType == testRowType1) {
                index[0]++;
                byte[] value = primaryRowMarshaller.valueFromRow(rowType, row, 0);
                long timestamp = primaryRowMarshaller.timestampFromRow(row, 0);
                Assert.assertEquals(value, UIO.intBytes(200 + index[0]));
                Assert.assertEquals(timestamp, 2000 + index[0]);
            }
            return true;
        });
        Assert.assertEquals(index[0], 4);

        deltaStripeWALStorage.merge(partitionIndex, partitionCreator, currentVersionProvider, false);

        index[0] = 0;
        deltaStripeWALStorage.takeRowsFromTransactionId(versionedPartitionName1, storage, prefixA, 0, (rowFP, rowTxId, rowType, row) -> {
            if (rowType == testRowType1) {
                index[0]++;
                byte[] value = primaryRowMarshaller.valueFromRow(rowType, row, 0);
                long timestamp = primaryRowMarshaller.timestampFromRow(row, 0);
                Assert.assertEquals(value, UIO.intBytes(100 + index[0]));
                Assert.assertEquals(timestamp, 1000 + index[0]);
            }
            return true;
        });
        Assert.assertEquals(index[0], 4);

        index[0] = 0;
        deltaStripeWALStorage.takeRowsFromTransactionId(versionedPartitionName1, storage, prefixB, 0, (rowFP, rowTxId, rowType, row) -> {
            if (rowType == testRowType1) {
                index[0]++;
                byte[] value = primaryRowMarshaller.valueFromRow(rowType, row, 0);
                long timestamp = primaryRowMarshaller.timestampFromRow(row, 0);
                Assert.assertEquals(value, UIO.intBytes(200 + index[0]));
                Assert.assertEquals(timestamp, 2000 + index[0]);
            }
            return true;
        });
        Assert.assertEquals(index[0], 4);
    }

    @Test(enabled = false)
    public void testCorrectness() throws Exception {
        WALStorage storage1 = partitionStore1.getWalStorage();

        deltaStripeWALStorage.update(true, testRowType1, highwaterStorage, versionedPartitionName1, partitionStore1, null,
            (highwaters, txKeyValueStream) -> {
                for (int i = 0; i < 10_000; i++) {
                    txKeyValueStream.row(-1, UIO.longBytes(i), UIO.longBytes(i), System.currentTimeMillis(), false, orderIdProvider.nextId());
                }
                return true;
            },
            updated);

        deltaStripeWALStorage.merge(partitionIndex, partitionCreator, currentVersionProvider, true);

        partitionIndex = new PartitionIndex(amzaStats, orderIdProvider, indexedWALStorageProvider, 4);
        partitionCreator = new PartitionCreator(orderIdProvider, partitionPropertyMarshaller, partitionIndex, systemWALStorage, updated, rowChanges, interner);
        partitionCreator.init((partitionName) -> 0);
        deltaStripeWALStorage = loadDeltaStripe();

        PartitionStore partitionStore = partitionCreator.get(versionedPartitionName1, 0);
        //TODO assert leaps and merge markers
    }

    private KeyContainedStream assertKeyIsContained(boolean shouldBeContained) {
        return (prefix, key, contained) -> {
            Assert.assertEquals(contained, shouldBeContained);
            return true;
        };
    }

    private WALKey key(byte[] prefix, int i) {
        return new WALKey(prefix, UIO.intBytes(i));
    }

    private UnprefixedWALKeys keys(int... lotsOfIs) {
        return stream -> {
            for (int i : lotsOfIs) {
                if (!stream.stream(UIO.intBytes(i))) {
                    return false;
                }
            }
            return true;
        };
    }

    static class IntUpdate implements Commitable {

        private final byte[] key;
        private final WALValue value;

        IntUpdate(RowType rowType, int key, int value, long timestamp, boolean delete) {
            this.key = UIO.intBytes(key);
            this.value = new WALValue(rowType, delete ? null : UIO.intBytes(value), timestamp, delete, timestamp);
        }

        @Override
        public boolean commitable(Highwaters highwaters, UnprefixedTxKeyValueStream keyValueStream) throws Exception {
            return keyValueStream.row(-1, key, value.getValue(), value.getTimestampId(), value.getTombstoned(), value.getVersion());
        }
    }

}
