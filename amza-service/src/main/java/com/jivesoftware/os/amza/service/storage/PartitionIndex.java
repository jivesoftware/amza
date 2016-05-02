package com.jivesoftware.os.amza.service.storage;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.service.IndexedWALStorageProvider;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.collections.lh.ConcurrentLHash;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

/**
 * @author jonathan.colt
 */
public class PartitionIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    // TODO consider replacing ConcurrentHashMap<Long, PartitionStore> LHash
    private final ConcurrentMap<PartitionName, ConcurrentLHash<PartitionStore>> partitionStores = Maps.newConcurrentMap();
    private final StripingLocksProvider<VersionedPartitionName> locksProvider = new StripingLocksProvider<>(1024); // TODO expose to config

    private final AmzaStats amzaStats;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final IndexedWALStorageProvider walStorageProvider;
    private final int concurrency;

    public PartitionIndex(AmzaStats amzaStats,
        TimestampedOrderIdProvider orderIdProvider,
        IndexedWALStorageProvider walStorageProvider,
        int concurrency) {

        this.amzaStats = amzaStats;
        this.orderIdProvider = orderIdProvider;
        this.walStorageProvider = walStorageProvider;
        this.concurrency = concurrency;
    }

    public PartitionStore getIfPresent(VersionedPartitionName versionedPartitionName) {
        ConcurrentLHash<PartitionStore> versionedStores = partitionStores.get(versionedPartitionName.getPartitionName());
        if (versionedStores != null) {
            return versionedStores.get(versionedPartitionName.getPartitionVersion());
        }
        return null;
    }

    public PartitionStore get(VersionedPartitionName versionedPartitionName, PartitionProperties properties, int stripe) throws Exception {
        return getAndValidate(-1, -1, versionedPartitionName, properties, stripe);
    }

    public PartitionStore getAndValidate(long deltaWALId,
        long prevDeltaWALId,
        VersionedPartitionName versionedPartitionName,
        PartitionProperties properties,
        int stripe) throws Exception {

        if (properties == null) {
            return null;
        }

        PartitionName partitionName = versionedPartitionName.getPartitionName();
        if (deltaWALId > -1 && partitionName.isSystemPartition()) {
            throw new IllegalStateException("Hooray you have a bug! Should never call get with something other than -1 for system parititions." + deltaWALId);
        }
        ConcurrentLHash<PartitionStore> versionedStores = partitionStores.get(partitionName);
        if (versionedStores != null) {
            PartitionStore partitionStore = versionedStores.get(versionedPartitionName.getPartitionVersion());
            if (partitionStore != null) {
                File baseKey = walStorageProvider.baseKey(versionedPartitionName, stripe);
                partitionStore.load(baseKey, deltaWALId, prevDeltaWALId, stripe);
                return partitionStore;
            }
        }

        if (!versionedPartitionName.getPartitionName().isSystemPartition()
            && !getSystemPartition(PartitionCreator.REGION_INDEX).containsKey(null, partitionName.toBytes())) {
            return null;
        }

        return init(deltaWALId, prevDeltaWALId, versionedPartitionName, stripe, properties);

    }

    public PartitionStore getSystemPartition(VersionedPartitionName versionedPartitionName) {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Should only be called by system partitions.");
        ConcurrentLHash<PartitionStore> versionedPartitionStores = partitionStores.get(versionedPartitionName.getPartitionName());
        PartitionStore store = versionedPartitionStores == null ? null : versionedPartitionStores.get(0L);
        if (store == null) {
            throw new IllegalStateException("There is no system partition for " + versionedPartitionName);
        }
        return store;
    }

    public void delete(VersionedPartitionName versionedPartitionName, int stripe) throws Exception {
        ConcurrentLHash<PartitionStore> versionedStores = partitionStores.get(versionedPartitionName.getPartitionName());
        if (versionedStores != null) {
            PartitionStore partitionStore = versionedStores.get(versionedPartitionName.getPartitionVersion());
            if (partitionStore != null) {
                File baseKey = walStorageProvider.baseKey(versionedPartitionName, stripe);
                partitionStore.delete(baseKey);
                versionedStores.remove(versionedPartitionName.getPartitionVersion());
            }
        }
    }

    private PartitionStore init(long deltaWALId,
        long prevDeltaWALId,
        VersionedPartitionName versionedPartitionName,
        int stripe,
        PartitionProperties properties) throws Exception {
        synchronized (locksProvider.lock(versionedPartitionName, 1234)) {
            ConcurrentLHash<PartitionStore> versionedStores = partitionStores.computeIfAbsent(versionedPartitionName.getPartitionName(),
                (key) -> new ConcurrentLHash<>(3, -1, -2, concurrency));

            PartitionStore partitionStore = versionedStores.get(versionedPartitionName.getPartitionVersion());
            if (partitionStore != null) {
                return partitionStore;
            }

            File baseKey = walStorageProvider.baseKey(versionedPartitionName, stripe);
            WALStorage<?> walStorage = walStorageProvider.create(versionedPartitionName, properties);
            partitionStore = new PartitionStore(amzaStats, orderIdProvider, versionedPartitionName, walStorage, properties);
            partitionStore.load(baseKey, deltaWALId, prevDeltaWALId, stripe);

            versionedStores.put(versionedPartitionName.getPartitionVersion(), partitionStore);
            LOG.info("Opened partition:" + versionedPartitionName);

            return partitionStore;
        }
    }

    public boolean exists(VersionedPartitionName versionedPartitionName, PartitionProperties properties, int stripe) throws Exception {
        return get(versionedPartitionName, properties, stripe) != null;
    }

    public void updateStoreProperties(PartitionName partitionName, PartitionProperties properties) throws Exception {
        ConcurrentLHash<PartitionStore> versionedPartitionStores = partitionStores.get(partitionName);
        if (versionedPartitionStores != null) {
            versionedPartitionStores.stream((long key, PartitionStore store) -> {
                store.updateProperties(properties);
                return true;
            });
        }
    }

    public interface PartitionStream {

        boolean stream(VersionedPartitionName versionedPartitionName) throws Exception;
    }

    public void streamActivePartitions(PartitionStream stream) throws Exception {
        if (stream != null) {
            for (Entry<PartitionName, ConcurrentLHash<PartitionStore>> entry : partitionStores.entrySet()) {
                if (!entry.getValue().stream((key, partitionStore) -> stream.stream(new VersionedPartitionName(entry.getKey(), key)))) {
                    break;
                }
            }
        }
    }

    public interface PartitionPropertiesStream {

        boolean stream(PartitionName partitionName, PartitionProperties partitionProperties) throws Exception;
    }

    public void invalidate(PartitionName partitionName) {
        partitionStores.remove(partitionName);
    }
}
