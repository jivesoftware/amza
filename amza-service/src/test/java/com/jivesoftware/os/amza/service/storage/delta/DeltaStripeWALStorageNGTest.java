package com.jivesoftware.os.amza.service.storage.delta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.PartitionTx;
import com.jivesoftware.os.amza.api.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.api.partition.StorageVersion;
import com.jivesoftware.os.amza.api.partition.TxPartitionState;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedState;
import com.jivesoftware.os.amza.api.partition.WALStorageDescriptor;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.UnprefixedTxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.service.IndexedWALStorageProvider;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.replication.PartitionBackedHighwaterStorage;
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
import com.jivesoftware.os.amza.shared.filer.HeapByteBufferFactory;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.shared.stream.KeyContainedStream;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.io.File;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class DeltaStripeWALStorageNGTest {

    private final WALUpdated updated = (versionedPartitionName, txId) -> {
    };

    private final VersionedPartitionName versionedPartitionName = new VersionedPartitionName(
        new PartitionName(false, "ring".getBytes(), "partitionName".getBytes()),
        VersionedPartitionName.STATIC_VERSION);
    private final BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller();
    private final BinaryHighwaterRowMarshaller highwaterRowMarshaller = new BinaryHighwaterRowMarshaller();

    private PartitionIndex partitionIndex;
    private PartitionStore partitionStore;
    private DeltaStripeWALStorage deltaStripeWALStorage;
    private HighwaterStorage highwaterStorage;

    @BeforeTest
    public void setup() throws Exception {
        OrderIdProviderImpl ids = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        ObjectMapper mapper = new ObjectMapper();
        JacksonPartitionPropertyMarshaller partitionPropertyMarshaller = new JacksonPartitionPropertyMarshaller(mapper);

        File partitionTmpDir = Files.createTempDir();
        String[] workingDirectories = {partitionTmpDir.getAbsolutePath()};
        IoStats ioStats = new IoStats();
        MemoryBackedRowIOProvider ephemeralRowIOProvider = new MemoryBackedRowIOProvider(workingDirectories,
            ioStats,
            100,
            1_024,
            1_024 * 1_024,
            new HeapByteBufferFactory());
        BinaryRowIOProvider persistentRowIOProvider = new BinaryRowIOProvider(workingDirectories,
            ioStats,
            100,
            false);
        WALIndexProviderRegistry walIndexProviderRegistry = new WALIndexProviderRegistry(ephemeralRowIOProvider, persistentRowIOProvider);

        IndexedWALStorageProvider indexedWALStorageProvider = new IndexedWALStorageProvider(
            walIndexProviderRegistry, primaryRowMarshaller, highwaterRowMarshaller, ids, -1);
        partitionIndex = new PartitionIndex(indexedWALStorageProvider,
            partitionPropertyMarshaller,
            false);

        Waterline waterline = new Waterline(null, State.follower, System.currentTimeMillis(), 0, true);
        LivelyEndState livelyEndState = new LivelyEndState(null, waterline, waterline, null);

        TxPartitionState txPartitionState = new TxPartitionState() {

            @Override
            public <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception {
                return tx.tx(new VersionedPartitionName(partitionName, 0), livelyEndState);
            }

            @Override
            public VersionedState getLocalVersionedState(PartitionName partitionName) throws Exception {
                return new VersionedState(livelyEndState, new StorageVersion(0, 0));
            }
        };

        partitionIndex.open(txPartitionState);

        SystemWALStorage systemWALStorage = new SystemWALStorage(partitionIndex,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            null,
            false);

        PartitionCreator partitionProvider = new PartitionCreator(ids,
            partitionPropertyMarshaller, partitionIndex, systemWALStorage, updated, partitionIndex);

        WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(false,
            new PrimaryIndexDescriptor("memory_persistent", 0, false, null), null, 100, 100);

        partitionProvider.createPartitionStoreIfAbsent(versionedPartitionName, new PartitionProperties(storageDescriptor, Consistency.none, true, 0, false,
            RowType.primary));
        partitionStore = partitionIndex.get(versionedPartitionName);
        Assert.assertNotNull(partitionStore);

        highwaterStorage = new PartitionBackedHighwaterStorage(ids, new RingMember("localhost"), partitionIndex, systemWALStorage, updated, 100);

        File tmp = Files.createTempDir();
        RowIOProvider<File> ioProvider = new BinaryRowIOProvider(new String[]{tmp.getAbsolutePath()}, ioStats, 100, false);
        DeltaWALFactory deltaWALFactory = new DeltaWALFactory(ids, tmp, ioProvider, primaryRowMarshaller, highwaterRowMarshaller);
        deltaStripeWALStorage = new DeltaStripeWALStorage(1, new AmzaStats(), deltaWALFactory, 0);
        deltaStripeWALStorage.load(txPartitionState, partitionIndex, primaryRowMarshaller);
    }

    @Test
    public void test() throws Exception {
        WALStorage storage = partitionStore.getWalStorage();
        byte[] prefix = UIO.intBytes(-1);
        WALKey walKey = key(prefix, 1);

        Assert.assertNull(deltaStripeWALStorage.get(versionedPartitionName, storage, walKey.prefix, walKey.key));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(false));
        Assert.assertEquals(storage.count(keyStream -> true), 0);
        Assert.assertNull(storage.getTimestampedValue(walKey.prefix, walKey.key));

        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefix, new IntUpdate(1, 1, 1, false), updated);

        deltaStripeWALStorage.get(versionedPartitionName, storage, prefix, keyStream -> keyStream.stream(walKey.key),
            (rowType, _prefix, key, value, timestamp, tombstoned, version) -> {
                Assert.assertFalse(tombstoned);
                Assert.assertEquals(key, walKey.key);
                Assert.assertEquals(value, UIO.intBytes(1));
                return true;
            });

        Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName, storage, walKey.prefix, walKey.key),
            new WALValue(RowType.primary, UIO.intBytes(1), 1, false, 1));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(true));
        Assert.assertEquals(storage.count(keyStream -> true), 0);
        Assert.assertNull(storage.getTimestampedValue(walKey.prefix, walKey.key));

        deltaStripeWALStorage.merge(partitionIndex, false);

        Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName, storage, walKey.prefix, walKey.key),
            new WALValue(RowType.primary, UIO.intBytes(1), 1, false, 1));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(true));
        Assert.assertEquals(storage.getTimestampedValue(walKey.prefix, walKey.key), new TimestampedValue(1, 1, UIO.intBytes(1)));
        Assert.assertEquals(storage.count(keyStream -> true), 1);

        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefix, new IntUpdate(1, 1, 0, false), updated);
        Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName, storage, walKey.prefix, walKey.key),
            new WALValue(RowType.primary, UIO.intBytes(1), 1, false, 1));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(true));
        Assert.assertEquals(storage.getTimestampedValue(walKey.prefix, walKey.key), new TimestampedValue(1, 1, UIO.intBytes(1)));
        Assert.assertEquals(storage.count(keyStream -> true), 1);

        deltaStripeWALStorage.merge(partitionIndex, false);

        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefix, new IntUpdate(1, 1, 2, true), updated);
        Assert.assertTrue(deltaStripeWALStorage.get(versionedPartitionName, storage, walKey.prefix, walKey.key).getTombstoned());
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(false));
        Assert.assertEquals(storage.getTimestampedValue(walKey.prefix, walKey.key), new TimestampedValue(1, 1, UIO.intBytes(1)));
        Assert.assertEquals(storage.count(keyStream -> true), 1);

        deltaStripeWALStorage.merge(partitionIndex, false);
        Assert.assertTrue(deltaStripeWALStorage.get(versionedPartitionName, storage, walKey.prefix, walKey.key).getTombstoned());
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(false));
        Assert.assertNull(storage.getTimestampedValue(walKey.prefix, walKey.key));
        Assert.assertEquals(storage.count(keyStream -> true), 1);

        storage.compactTombstone(RowType.primary, 10, Long.MAX_VALUE, false);
        storage.compactTombstone(RowType.primary, 10, Long.MAX_VALUE, false); // Bla

        Assert.assertEquals(storage.count(keyStream -> true), 0);
    }

    @Test
    public void testTombstones() throws Exception {
        WALStorage storage = partitionStore.getWalStorage();
        byte[] prefix = UIO.intBytes(-1);
        byte[] key1 = UIO.intBytes(1);
        byte[] key2 = UIO.intBytes(2);

        Assert.assertNull(deltaStripeWALStorage.get(versionedPartitionName, storage, prefix, key1));
        Assert.assertNull(deltaStripeWALStorage.get(versionedPartitionName, storage, prefix, key2));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(false));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(2), assertKeyIsContained(false));
        Assert.assertNull(storage.getTimestampedValue(prefix, key1));
        Assert.assertNull(storage.getTimestampedValue(prefix, key2));

        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefix, new IntUpdate(1, 1, 1, false), updated);
        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefix, new IntUpdate(2, 2, 1, false), updated);

        Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName, storage, prefix, key1),
            new WALValue(RowType.primary, UIO.intBytes(1), 1, false, 1));
        Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName, storage, prefix, key2),
            new WALValue(RowType.primary, UIO.intBytes(2), 1, false, 1));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(true));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(2), assertKeyIsContained(true));

        deltaStripeWALStorage.get(versionedPartitionName, storage, prefix, keys(1), (rowType, _prefix, key, value, timestamp, tombstoned, version) -> {
            Assert.assertFalse(tombstoned);
            Assert.assertEquals(key, key1);
            Assert.assertEquals(value, UIO.intBytes(1));
            return true;
        });
        deltaStripeWALStorage.get(versionedPartitionName, storage, prefix, keys(2), (rowType, _prefix, key, value, timestamp, tombstoned, version) -> {
            Assert.assertFalse(tombstoned);
            Assert.assertEquals(key, key2);
            Assert.assertEquals(value, UIO.intBytes(2));
            return true;
        });

        deltaStripeWALStorage.merge(partitionIndex, false);

        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefix, new IntUpdate(1, 1, 2, true), updated);
        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefix, new IntUpdate(2, 2, 2, true), updated);

        Assert.assertTrue(deltaStripeWALStorage.get(versionedPartitionName, storage, prefix, key1).getTombstoned());
        Assert.assertTrue(deltaStripeWALStorage.get(versionedPartitionName, storage, prefix, key2).getTombstoned());
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(false));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(2), assertKeyIsContained(false));

        deltaStripeWALStorage.get(versionedPartitionName, storage, prefix, keys(1), (rowType, _prefix, key, value, timestamp, tombstoned, version) -> {
            Assert.assertTrue(tombstoned);
            Assert.assertEquals(key, key1);
            Assert.assertNull(value);
            Assert.assertEquals(timestamp, 2);
            return true;
        });
        deltaStripeWALStorage.get(versionedPartitionName, storage, prefix, keys(2), (rowType, _prefix, key, value, timestamp, tombstoned, version) -> {
            Assert.assertTrue(tombstoned);
            Assert.assertEquals(key, key2);
            Assert.assertNull(value);
            Assert.assertEquals(timestamp, 2);
            return true;
        });
    }

    @Test
    public void testTakeWithPrefix() throws Exception {
        WALStorage storage = partitionStore.getWalStorage();
        byte[] prefixA = "a".getBytes();
        byte[] prefixB = "b".getBytes();

        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefixA, new IntUpdate(1, 101, 1001, false),
            updated);
        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefixB, new IntUpdate(1, 201, 2001, false),
            updated);
        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefixA, new IntUpdate(2, 102, 1002, false),
            updated);
        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefixB, new IntUpdate(2, 202, 2002, false),
            updated);
        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefixA, new IntUpdate(3, 103, 1003, false),
            updated);
        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefixB, new IntUpdate(3, 203, 2003, false),
            updated);
        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefixA, new IntUpdate(4, 104, 1004, false),
            updated);
        deltaStripeWALStorage.update(RowType.primary, highwaterStorage, versionedPartitionName, storage, prefixB, new IntUpdate(4, 204, 2004, false),
            updated);

        int[] index = new int[1];
        deltaStripeWALStorage.takeRowsFromTransactionId(versionedPartitionName, storage, prefixA, 0, (rowFP, rowTxId, rowType, row) -> {
            if (rowType == RowType.primary) {
                index[0]++;
                byte[] value = primaryRowMarshaller.valueFromRow(RowType.primary, row, 0);
                long timestamp = primaryRowMarshaller.timestampFromRow(row, 0);
                Assert.assertEquals(value, UIO.intBytes(100 + index[0]));
                Assert.assertEquals(timestamp, 1000 + index[0]);
            }
            return true;
        });
        Assert.assertEquals(index[0], 4);

        index[0] = 0;
        deltaStripeWALStorage.takeRowsFromTransactionId(versionedPartitionName, storage, prefixB, 0, (rowFP, rowTxId, rowType, row) -> {
            if (rowType == RowType.primary) {
                index[0]++;
                byte[] value = primaryRowMarshaller.valueFromRow(RowType.primary, row, 0);
                long timestamp = primaryRowMarshaller.timestampFromRow(row, 0);
                Assert.assertEquals(value, UIO.intBytes(200 + index[0]));
                Assert.assertEquals(timestamp, 2000 + index[0]);
            }
            return true;
        });
        Assert.assertEquals(index[0], 4);

        deltaStripeWALStorage.merge(partitionIndex, false);

        index[0] = 0;
        deltaStripeWALStorage.takeRowsFromTransactionId(versionedPartitionName, storage, prefixA, 0, (rowFP, rowTxId, rowType, row) -> {
            if (rowType == RowType.primary) {
                index[0]++;
                byte[] value = primaryRowMarshaller.valueFromRow(RowType.primary, row, 0);
                long timestamp = primaryRowMarshaller.timestampFromRow(row, 0);
                Assert.assertEquals(value, UIO.intBytes(100 + index[0]));
                Assert.assertEquals(timestamp, 1000 + index[0]);
            }
            return true;
        });
        Assert.assertEquals(index[0], 4);

        index[0] = 0;
        deltaStripeWALStorage.takeRowsFromTransactionId(versionedPartitionName, storage, prefixB, 0, (rowFP, rowTxId, rowType, row) -> {
            if (rowType == RowType.primary) {
                index[0]++;
                byte[] value = primaryRowMarshaller.valueFromRow(RowType.primary, row, 0);
                long timestamp = primaryRowMarshaller.timestampFromRow(row, 0);
                Assert.assertEquals(value, UIO.intBytes(200 + index[0]));
                Assert.assertEquals(timestamp, 2000 + index[0]);
            }
            return true;
        });
        Assert.assertEquals(index[0], 4);
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

        IntUpdate(int key, int value, long timestamp, boolean delete) {
            this.key = UIO.intBytes(key);
            this.value = new WALValue(RowType.primary, delete ? null : UIO.intBytes(value), timestamp, delete, timestamp);
        }

        @Override
        public boolean commitable(Highwaters highwaters, UnprefixedTxKeyValueStream keyValueStream) throws Exception {
            return keyValueStream.row(-1, key, value.getValue(), value.getTimestampId(), value.getTombstoned(), value.getVersion());
        }
    }

}
