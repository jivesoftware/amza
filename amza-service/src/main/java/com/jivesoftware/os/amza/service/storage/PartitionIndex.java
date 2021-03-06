package com.jivesoftware.os.amza.service.storage;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.service.IndexedWALStorageProvider;
import com.jivesoftware.os.amza.service.StripingLocksProvider;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.jive.utils.collections.lh.ConcurrentLHash;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

/**
 * @author jonathan.colt
 */
public class PartitionIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    // TODO consider replacing ConcurrentHashMap<Long, PartitionStore> LHash
    private final ConcurrentMap<PartitionName, ConcurrentLHash<PartitionStore>> partitionStores = Maps.newConcurrentMap();
    private final StripingLocksProvider<VersionedPartitionName> locksProvider = new StripingLocksProvider<>(1024); // TODO expose to config

    private final AmzaStats amzaSystemStats;
    private final AmzaStats amzaStats;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final IndexedWALStorageProvider walStorageProvider;
    private final int concurrency;
    private final ExecutorService partitionLoadExecutorService;

    public PartitionIndex(AmzaStats amzaSystemStats,
        AmzaStats amzaStats,
        TimestampedOrderIdProvider orderIdProvider,
        IndexedWALStorageProvider walStorageProvider,
        int concurrency,
        ExecutorService partitionLoadExecutorService) {

        this.amzaSystemStats = amzaSystemStats;
        this.amzaStats = amzaStats;
        this.orderIdProvider = orderIdProvider;
        this.walStorageProvider = walStorageProvider;
        this.concurrency = concurrency;
        this.partitionLoadExecutorService = partitionLoadExecutorService;
    }

    public void start() {
    }

    public void stop() {
        partitionLoadExecutorService.shutdownNow();
    }

    public PartitionStore getIfPresent(VersionedPartitionName versionedPartitionName) {
        ConcurrentLHash<PartitionStore> versionedStores = partitionStores.get(versionedPartitionName.getPartitionName());
        if (versionedStores != null) {
            return versionedStores.get(versionedPartitionName.getPartitionVersion());
        }
        return null;
    }

    public PartitionStore get(String context, VersionedPartitionName versionedPartitionName, PartitionProperties properties, int stripe) throws Exception {
        return getAndValidate(context, -1, -1, versionedPartitionName, properties, stripe);
    }

    public PartitionStore getAndValidate(String context,
        long deltaWALId,
        long prevDeltaWALId,
        VersionedPartitionName versionedPartitionName,
        PartitionProperties properties,
        int stripe) throws Exception {

        if (properties == null) {
            return null;
        }

        PartitionName partitionName = versionedPartitionName.getPartitionName();
        if (deltaWALId > -1 && partitionName.isSystemPartition()) {
            throw new IllegalStateException("Hooray you have a bug! Should never call get with something other than -1 for system partitions: " + deltaWALId);
        }
        ConcurrentLHash<PartitionStore> versionedStores = partitionStores.get(partitionName);
        if (versionedStores != null) {
            PartitionStore partitionStore = versionedStores.get(versionedPartitionName.getPartitionVersion());
            if (partitionStore != null) {
                File baseKey = walStorageProvider.baseKey(versionedPartitionName, stripe);
                partitionStore.load(baseKey, deltaWALId, prevDeltaWALId, stripe, partitionLoadExecutorService);
                return partitionStore;
            }
        }

        if (!versionedPartitionName.getPartitionName().isSystemPartition()
            && !getSystemPartition(PartitionCreator.REGION_INDEX).containsKey(null, partitionName.toBytes())) {
            return null;
        }

        return init(context, deltaWALId, prevDeltaWALId, versionedPartitionName, stripe, properties);

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

    private PartitionStore init(String context,
        long deltaWALId,
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
            partitionStore = new PartitionStore(versionedPartitionName.getPartitionName().isSystemPartition() ? amzaSystemStats : amzaStats,
                orderIdProvider, versionedPartitionName, walStorage, properties);
            partitionStore.load(baseKey, deltaWALId, prevDeltaWALId, stripe, partitionLoadExecutorService);

            versionedStores.put(versionedPartitionName.getPartitionVersion(), partitionStore);
            LOG.info("Opened partition:" + versionedPartitionName);
            LOG.inc("open>context>" + context);

            return partitionStore;
        }
    }

    public boolean exists(String context, VersionedPartitionName versionedPartitionName, PartitionProperties properties, int stripe) throws Exception {
        return get(context, versionedPartitionName, properties, stripe) != null;
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
