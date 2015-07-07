package com.jivesoftware.os.amza.service.storage.delta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.service.IndexedWALStorageProvider;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.replication.MemoryBackedHighwaterStorage;
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
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.PartitionTx;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.stats.IoStats;
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
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class DeltaStripeWALStorageNGTest {

    private final WALUpdated updated = (VersionedPartitionName versionedPartitionName, Status partitionStatus, long txId) -> {
    };

    @Test
    public void test() throws Exception {

        RowIOProvider rowIOProvider = new BinaryRowIOProvider(new IoStats(), 100, false);
        BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller();
        BinaryHighwaterRowMarshaller highwaterRowMarshaller = new BinaryHighwaterRowMarshaller();
        OrderIdProviderImpl ids = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        ObjectMapper mapper = new ObjectMapper();
        JacksonPartitionPropertyMarshaller partitionPropertyMarshaller = new JacksonPartitionPropertyMarshaller(mapper);

        File partitionTmpDir = Files.createTempDir();
        WALIndexProviderRegistry walIndexProviderRegistry = new WALIndexProviderRegistry();

        IndexedWALStorageProvider indexedWALStorageProvider = new IndexedWALStorageProvider(
            walIndexProviderRegistry, rowIOProvider, primaryRowMarshaller, highwaterRowMarshaller, ids, -1, -1);
        PartitionIndex partitionIndex = new PartitionIndex(
            new String[] { partitionTmpDir.getAbsolutePath() },
            "domain",
            indexedWALStorageProvider,
            partitionPropertyMarshaller,
            false);

        partitionIndex.open(new TxPartitionStatus() {

            @Override
            public <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception {
                return tx.tx(new VersionedPartitionName(partitionName, 0), TxPartitionStatus.Status.ONLINE);
            }
        });

        SystemWALStorage systemWALStorage = new SystemWALStorage(partitionIndex, null, false);

        PartitionProvider partitionProvider = new PartitionProvider(ids,
            partitionPropertyMarshaller, partitionIndex, systemWALStorage, updated, partitionIndex);

        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "ring", "partitionName"), 1);
        WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
            new PrimaryIndexDescriptor("memory", 0, false, null), null, 100, 100);

        partitionProvider.createPartitionStoreIfAbsent(versionedPartitionName, new PartitionProperties(storageDescriptor, 0, false));
        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
        Assert.assertNotNull(partitionStore);

        HighwaterStorage highwaterStorage = new MemoryBackedHighwaterStorage();

        File tmp = Files.createTempDir();
        DeltaWALFactory deltaWALFactory = new DeltaWALFactory(ids, tmp, rowIOProvider, primaryRowMarshaller, highwaterRowMarshaller, -1);
        DeltaValueCache deltaValueCache = new DeltaValueCache(1_000);
        DeltaStripeWALStorage deltaStripeWALStorage = new DeltaStripeWALStorage(1, new AmzaStats(), deltaWALFactory, deltaValueCache, 0);
        deltaStripeWALStorage.load(partitionIndex, primaryRowMarshaller);

        WALStorage storage = partitionStore.getWalStorage();
        Assert.assertNull(deltaStripeWALStorage.get(versionedPartitionName, storage, key(1)));
        Assert.assertFalse(deltaStripeWALStorage.containsKey(versionedPartitionName, storage, key(1)));
        Assert.assertEquals(0, storage.count());
        Assert.assertNull(storage.get(key(1)));

        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, new IntUpdate(1, 1, false), updated);

        Assert.assertEquals(new WALValue(UIO.intBytes(1), 1, false), deltaStripeWALStorage.get(versionedPartitionName, storage, key(1)));
        Assert.assertTrue(deltaStripeWALStorage.containsKey(versionedPartitionName, storage, key(1)));
        Assert.assertEquals(0, storage.count());
        Assert.assertNull(storage.get(key(1)));

        deltaStripeWALStorage.compact(partitionIndex, false);

        Assert.assertEquals(new WALValue(UIO.intBytes(1), 1, false), deltaStripeWALStorage.get(versionedPartitionName, storage, key(1)));
        Assert.assertTrue(deltaStripeWALStorage.containsKey(versionedPartitionName, storage, key(1)));
        Assert.assertEquals(new WALValue(UIO.intBytes(1), 1, false), storage.get(key(1)));
        Assert.assertEquals(1, storage.count());

        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, new IntUpdate(1, 0, false), updated);
        Assert.assertEquals(new WALValue(UIO.intBytes(1), 1, false), deltaStripeWALStorage.get(versionedPartitionName, storage, key(1)));
        Assert.assertTrue(deltaStripeWALStorage.containsKey(versionedPartitionName, storage, key(1)));
        Assert.assertEquals(new WALValue(UIO.intBytes(1), 1, false), storage.get(key(1)));
        Assert.assertEquals(1, storage.count());

        deltaStripeWALStorage.compact(partitionIndex, false);

        deltaStripeWALStorage.update(highwaterStorage, versionedPartitionName, Status.ONLINE, storage, new IntUpdate(1, 2, true), updated);
        Assert.assertNull(deltaStripeWALStorage.get(versionedPartitionName, storage, key(1)));
        Assert.assertFalse(deltaStripeWALStorage.containsKey(versionedPartitionName, storage, key(1)));
        Assert.assertEquals(new WALValue(UIO.intBytes(1), 1, false), storage.get(key(1)));
        Assert.assertEquals(1, storage.count());

        deltaStripeWALStorage.compact(partitionIndex, false);
        Assert.assertNull(deltaStripeWALStorage.get(versionedPartitionName, storage, key(1)));
        Assert.assertFalse(deltaStripeWALStorage.containsKey(versionedPartitionName, storage, key(1)));
        Assert.assertNull(storage.get(key(1)));
        Assert.assertEquals(1, storage.count());

        storage.compactTombstone(10, Long.MAX_VALUE);
        storage.compactTombstone(10, Long.MAX_VALUE); // Bla

        Assert.assertEquals(0, storage.count());

    }

    private WALKey key(int i) {
        return new WALKey(UIO.intBytes(i));
    }

    static class IntUpdate implements Commitable<WALValue> {

        private final WALKey key;
        private final WALValue value;

        IntUpdate(int i, long timestamp, boolean delete) {
            key = new WALKey(UIO.intBytes(i));
            value = new WALValue(UIO.intBytes(i), timestamp, delete);
        }

        @Override
        public void commitable(Highwaters highwaters, Scan<WALValue> scan) throws Exception {
            scan.row(-1, key, value);
        }
    }

}
