package com.jivesoftware.os.amza.service.storage.delta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.service.IndexedWALStorageProvider;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.replication.MemoryBackedHighwaterStorage;
import com.jivesoftware.os.amza.service.storage.JacksonRegionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.Commitable;
import com.jivesoftware.os.amza.shared.HighwaterStorage;
import com.jivesoftware.os.amza.shared.Highwaters;
import com.jivesoftware.os.amza.shared.NoOpWALReplicator;
import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RegionTx;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.TxRegionStatus;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.storage.binary.RowIOProvider;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.io.File;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class DeltaStripeWALStorageNGTest {

    @Test
    public void test() throws Exception {

        RowIOProvider rowIOProvider = new BinaryRowIOProvider(new IoStats(), 100, false);
        BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller();
        BinaryHighwaterRowMarshaller highwaterRowMarshaller = new BinaryHighwaterRowMarshaller();
        OrderIdProviderImpl ids = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        ObjectMapper mapper = new ObjectMapper();
        JacksonRegionPropertyMarshaller regionPropertyMarshaller = new JacksonRegionPropertyMarshaller(mapper);

        File regionTmpDir = Files.createTempDir();
        WALIndexProviderRegistry walIndexProviderRegistry = new WALIndexProviderRegistry();
        IndexedWALStorageProvider indexedWALStorageProvider = new IndexedWALStorageProvider(walIndexProviderRegistry,
            rowIOProvider, primaryRowMarshaller, highwaterRowMarshaller, ids, -1, -1);
        RegionIndex regionIndex = new RegionIndex(
            new AmzaStats(),
            new String[]{regionTmpDir.getAbsolutePath()},
            "domain",
            indexedWALStorageProvider,
            regionPropertyMarshaller,
            false);

        regionIndex.open(new TxRegionStatus() {

            @Override
            public <R> R tx(RegionName regionName, RegionTx<R> tx) throws Exception {
                return tx.tx(new VersionedRegionName(regionName, 0), TxRegionStatus.Status.ONLINE);
            }
        });

        RegionProvider regionProvider = new RegionProvider(ids, regionPropertyMarshaller, regionIndex, regionIndex, false);

        VersionedRegionName versionedRegionName = new VersionedRegionName(new RegionName(false, "ring", "regionName"), 1);
        WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
            new PrimaryIndexDescriptor("memory", 0, false, null), null, 100, 100);

        regionProvider.createRegionStoreIfAbsent(versionedRegionName, new RegionProperties(storageDescriptor, 0, 0, false));
        RegionStore regionStore = regionIndex.get(versionedRegionName);
        Assert.assertNotNull(regionStore);

        HighwaterStorage highwaterStorage = new MemoryBackedHighwaterStorage();

        File tmp = Files.createTempDir();
        DeltaWALFactory deltaWALFactory = new DeltaWALFactory(ids, tmp, rowIOProvider, primaryRowMarshaller, highwaterRowMarshaller, -1);
        DeltaStripeWALStorage deltaStripeWALStorage = new DeltaStripeWALStorage(highwaterStorage, 1, primaryRowMarshaller, highwaterRowMarshaller,
            deltaWALFactory, 0);
        deltaStripeWALStorage.load(regionIndex);

        WALReplicator replicator = new NoOpWALReplicator();

        WALStorage storage = regionStore.getWalStorage();
        Assert.assertNull(deltaStripeWALStorage.get(versionedRegionName, storage, key(1)));
        Assert.assertFalse(deltaStripeWALStorage.containsKey(versionedRegionName, storage, key(1)));
        Assert.assertEquals(0, storage.count());
        Assert.assertNull(storage.get(key(1)));

        deltaStripeWALStorage.update(versionedRegionName, storage, new IntUpdate(1, 1, false));

        Assert.assertEquals(new WALValue(UIO.intBytes(1), 1, false), deltaStripeWALStorage.get(versionedRegionName, storage, key(1)));
        Assert.assertTrue(deltaStripeWALStorage.containsKey(versionedRegionName, storage, key(1)));
        Assert.assertEquals(0, storage.count());
        Assert.assertNull(storage.get(key(1)));

        deltaStripeWALStorage.compact(regionIndex);

        Assert.assertEquals(new WALValue(UIO.intBytes(1), 1, false), deltaStripeWALStorage.get(versionedRegionName, storage, key(1)));
        Assert.assertTrue(deltaStripeWALStorage.containsKey(versionedRegionName, storage, key(1)));
        Assert.assertEquals(new WALValue(UIO.intBytes(1), 1, false), storage.get(key(1)));
        Assert.assertEquals(1, storage.count());

        deltaStripeWALStorage.update(versionedRegionName, storage, new IntUpdate(1, 0, false));
        Assert.assertEquals(new WALValue(UIO.intBytes(1), 1, false), deltaStripeWALStorage.get(versionedRegionName, storage, key(1)));
        Assert.assertTrue(deltaStripeWALStorage.containsKey(versionedRegionName, storage, key(1)));
        Assert.assertEquals(new WALValue(UIO.intBytes(1), 1, false), storage.get(key(1)));
        Assert.assertEquals(1, storage.count());

        deltaStripeWALStorage.compact(regionIndex);

        deltaStripeWALStorage.update(versionedRegionName, storage, new IntUpdate(1, 2, true));
        Assert.assertNull(deltaStripeWALStorage.get(versionedRegionName, storage, key(1)));
        Assert.assertFalse(deltaStripeWALStorage.containsKey(versionedRegionName, storage, key(1)));
        Assert.assertEquals(new WALValue(UIO.intBytes(1), 1, false), storage.get(key(1)));
        Assert.assertEquals(1, storage.count());

        deltaStripeWALStorage.compact(regionIndex);
        Assert.assertNull(deltaStripeWALStorage.get(versionedRegionName, storage, key(1)));
        Assert.assertFalse(deltaStripeWALStorage.containsKey(versionedRegionName, storage, key(1)));
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
