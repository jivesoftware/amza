package com.jivesoftware.os.amza.service.storage.delta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.service.IndexedWALStorageProvider;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.replication.PartitionBackedHighwaterStorage;
import com.jivesoftware.os.amza.service.storage.JacksonPartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.PartitionTx;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedStatus;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.shared.stream.KeyContainedStream;
import com.jivesoftware.os.amza.shared.stream.UnprefixedTxKeyValueStream;
import com.jivesoftware.os.amza.shared.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;
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

    private final WALUpdated updated = (VersionedPartitionName versionedPartitionName, Status partitionStatus, long txId) -> {
    };

    private final VersionedPartitionName versionedPartitionName =
        new VersionedPartitionName(new PartitionName(false, "ring".getBytes(), "partitionName".getBytes()), 1);
    private final BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller();
    private final BinaryHighwaterRowMarshaller highwaterRowMarshaller = new BinaryHighwaterRowMarshaller();

    private PartitionIndex partitionIndex;
    private PartitionStore partitionStore;
    private DeltaStripeWALStorage deltaStripeWALStorage;
    private HighwaterStorage highwaterStorage;

    @BeforeTest
    public void setup() throws Exception {
        RowIOProvider rowIOProvider = new BinaryRowIOProvider(new IoStats(), 100, false);
        OrderIdProviderImpl ids = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        ObjectMapper mapper = new ObjectMapper();
        JacksonPartitionPropertyMarshaller partitionPropertyMarshaller = new JacksonPartitionPropertyMarshaller(mapper);

        File partitionTmpDir = Files.createTempDir();
        WALIndexProviderRegistry walIndexProviderRegistry = new WALIndexProviderRegistry();

        IndexedWALStorageProvider indexedWALStorageProvider = new IndexedWALStorageProvider(
            walIndexProviderRegistry, rowIOProvider, primaryRowMarshaller, highwaterRowMarshaller, ids, -1, -1);
        partitionIndex = new PartitionIndex(
            new String[] { partitionTmpDir.getAbsolutePath() },
            "domain",
            indexedWALStorageProvider,
            partitionPropertyMarshaller,
            false);
        TxPartitionStatus txPartitionStatus = new TxPartitionStatus() {

            @Override
            public <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception {
                return tx.tx(new VersionedPartitionName(partitionName, 0), TxPartitionStatus.Status.ONLINE);
            }

            @Override
            public VersionedStatus getLocalStatus(PartitionName partitionName) throws Exception {
                return new VersionedStatus(Status.ONLINE, 0, 0);
            }
        };

        partitionIndex.open(txPartitionStatus);

        SystemWALStorage systemWALStorage = new SystemWALStorage(partitionIndex,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            null,
            false);

        PartitionProvider partitionProvider = new PartitionProvider(ids,
            partitionPropertyMarshaller, partitionIndex, systemWALStorage, updated, partitionIndex);


        WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
            new PrimaryIndexDescriptor("memory", 0, false, null), null, 100, 100);

        partitionProvider.createPartitionStoreIfAbsent(versionedPartitionName, new PartitionProperties(storageDescriptor, 0, false));
        partitionStore = partitionIndex.get(versionedPartitionName);
        Assert.assertNotNull(partitionStore);

        highwaterStorage = new PartitionBackedHighwaterStorage(ids, new RingMember("localhost"), systemWALStorage, updated, 100);

        File tmp = Files.createTempDir();
        DeltaWALFactory deltaWALFactory = new DeltaWALFactory(ids, tmp, rowIOProvider, primaryRowMarshaller, highwaterRowMarshaller, -1);
        deltaStripeWALStorage = new DeltaStripeWALStorage(1, new AmzaStats(), deltaWALFactory, 0);
        deltaStripeWALStorage.load(txPartitionStatus, partitionIndex, primaryRowMarshaller);
    }

    @Test
    public void test() throws Exception {
        WALStorage storage = partitionStore.getWalStorage();
        byte[] prefix = UIO.intBytes(-1);
        WALKey walKey = key(prefix, 1);

        Assert.assertNull(deltaStripeWALStorage.get(versionedPartitionName, storage, walKey.prefix, walKey.key));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(false));
        Assert.assertEquals(storage.count(keyStream -> true), 0);
        Assert.assertNull(storage.get(walKey.prefix, walKey.key));

        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, prefix, new IntUpdate(1, 1, 1, false), updated);

        Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName, storage, walKey.prefix, walKey.key), new WALValue(UIO.intBytes(1), 1, false));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(true));
        Assert.assertEquals(storage.count(keyStream -> true), 0);
        Assert.assertNull(storage.get(walKey.prefix, walKey.key));

        deltaStripeWALStorage.merge(partitionIndex, false);

        Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName, storage, walKey.prefix, walKey.key), new WALValue(UIO.intBytes(1), 1, false));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(true));
        Assert.assertEquals(storage.get(walKey.prefix, walKey.key), new TimestampedValue(1, UIO.intBytes(1)));
        Assert.assertEquals(storage.count(keyStream -> true), 1);

        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, prefix, new IntUpdate(1, 1, 0, false), updated);
        Assert.assertEquals(deltaStripeWALStorage.get(versionedPartitionName, storage, walKey.prefix, walKey.key), new WALValue(UIO.intBytes(1), 1, false));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(true));
        Assert.assertEquals(storage.get(walKey.prefix, walKey.key), new TimestampedValue(1, UIO.intBytes(1)));
        Assert.assertEquals(storage.count(keyStream -> true), 1);

        deltaStripeWALStorage.merge(partitionIndex, false);

        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, prefix, new IntUpdate(1, 1, 2, true), updated);
        Assert.assertNull(deltaStripeWALStorage.get(versionedPartitionName, storage, walKey.prefix, walKey.key));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(false));
        Assert.assertEquals(storage.get(walKey.prefix, walKey.key), new TimestampedValue(1, UIO.intBytes(1)));
        Assert.assertEquals(storage.count(keyStream -> true), 1);

        deltaStripeWALStorage.merge(partitionIndex, false);
        Assert.assertNull(deltaStripeWALStorage.get(versionedPartitionName, storage, walKey.prefix, walKey.key));
        deltaStripeWALStorage.containsKeys(versionedPartitionName, storage, prefix, keys(1), assertKeyIsContained(false));
        Assert.assertNull(storage.get(walKey.prefix, walKey.key));
        Assert.assertEquals(storage.count(keyStream -> true), 1);

        storage.compactTombstone(10, Long.MAX_VALUE, false);
        storage.compactTombstone(10, Long.MAX_VALUE, false); // Bla

        Assert.assertEquals(storage.count(keyStream -> true), 0);
    }

    @Test
    public void testTakeWithPrefix() throws Exception {
        WALStorage storage = partitionStore.getWalStorage();
        byte[] prefixA = "a".getBytes();
        byte[] prefixB = "b".getBytes();

        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, prefixA, new IntUpdate(1, 101, 1001, false), updated);
        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, prefixB, new IntUpdate(1, 201, 2001, false), updated);
        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, prefixA, new IntUpdate(2, 102, 1002, false), updated);
        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, prefixB, new IntUpdate(2, 202, 2002, false), updated);
        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, prefixA, new IntUpdate(3, 103, 1003, false), updated);
        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, prefixB, new IntUpdate(3, 203, 2003, false), updated);
        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, prefixA, new IntUpdate(4, 104, 1004, false), updated);
        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, prefixB, new IntUpdate(4, 204, 2004, false), updated);

        int[] index = new int[1];
        deltaStripeWALStorage.takeRowsFromTransactionId(versionedPartitionName, storage, prefixA, 0, (rowFP, rowTxId, rowType, row) -> {
            if (rowType == RowType.primary) {
                index[0]++;
                byte[] value = primaryRowMarshaller.valueFromRow(row);
                long timestamp = primaryRowMarshaller.timestampFromRow(row);
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
                byte[] value = primaryRowMarshaller.valueFromRow(row);
                long timestamp = primaryRowMarshaller.timestampFromRow(row);
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
                byte[] value = primaryRowMarshaller.valueFromRow(row);
                long timestamp = primaryRowMarshaller.timestampFromRow(row);
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
                byte[] value = primaryRowMarshaller.valueFromRow(row);
                long timestamp = primaryRowMarshaller.timestampFromRow(row);
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
            this.value = new WALValue(UIO.intBytes(value), timestamp, delete);
        }

        @Override
        public boolean commitable(Highwaters highwaters, UnprefixedTxKeyValueStream keyValueStream) throws Exception {
            return keyValueStream.row(-1, key, value.getValue(), value.getTimestampId(), value.getTombstoned());
        }
    }

}
