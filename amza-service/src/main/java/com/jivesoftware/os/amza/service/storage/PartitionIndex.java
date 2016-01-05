package com.jivesoftware.os.amza.service.storage;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.api.partition.RingMembership;
import com.jivesoftware.os.amza.api.partition.TxPartitionState;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.partition.WALStorageDescriptor;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALValue;
import com.jivesoftware.os.amza.service.IndexedWALStorageProvider;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionProvider;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jivesoftware.os.amza.service.storage.PartitionCreator.AQUARIUM_LIVELINESS_INDEX;
import static com.jivesoftware.os.amza.service.storage.PartitionCreator.AQUARIUM_STATE_INDEX;
import static com.jivesoftware.os.amza.service.storage.PartitionCreator.HIGHWATER_MARK_INDEX;
import static com.jivesoftware.os.amza.service.storage.PartitionCreator.REGION_PROPERTIES;

/**
 * @author jonathan.colt
 */
public class PartitionIndex implements RowChanges, VersionedPartitionProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ConcurrentHashMap<PartitionName, ConcurrentHashMap<Long, PartitionStore>> partitionStores = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PartitionName, PartitionProperties> partitionProperties = new ConcurrentHashMap<>();
    private final StripingLocksProvider<VersionedPartitionName> locksProvider = new StripingLocksProvider<>(1024); // TODO expose to config

    private final IndexedWALStorageProvider walStorageProvider;
    private final PartitionPropertyMarshaller partitionPropertyMarshaller;
    private final boolean hardFlush;

    public PartitionIndex(IndexedWALStorageProvider walStorageProvider,
        PartitionPropertyMarshaller partitionPropertyMarshaller,
        boolean hardFlush) {

        this.walStorageProvider = walStorageProvider;
        this.partitionPropertyMarshaller = partitionPropertyMarshaller;
        this.hardFlush = hardFlush;
    }

    public void open(TxPartitionState txPartitionState, RingMembership ringMembership) throws Exception {

        PartitionStore partitionIndexStore = get(PartitionCreator.REGION_INDEX);
        get(PartitionCreator.RING_INDEX);
        get(PartitionCreator.NODE_INDEX);
        get(PartitionCreator.HIGHWATER_MARK_INDEX);
        get(PartitionCreator.PARTITION_VERSION_INDEX);
        get(PartitionCreator.REGION_PROPERTIES);
        get(PartitionCreator.AQUARIUM_STATE_INDEX);
        get(PartitionCreator.AQUARIUM_LIVELINESS_INDEX);

        final ExecutorService openExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2,
            new ThreadFactoryBuilder().setNameFormat("open-index-%d").build());
        final AtomicInteger numOpened = new AtomicInteger(0);
        final AtomicInteger numFailed = new AtomicInteger(0);
        final AtomicInteger total = new AtomicInteger(0);
        partitionIndexStore.rowScan((rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
            final PartitionName partitionName = PartitionName.fromBytes(key);
            if (ringMembership.isMemberOfRing(partitionName.getRingName())) {
                try {
                    total.incrementAndGet();
                    openExecutor.submit(() -> {
                        try {
                            txPartitionState.tx(partitionName, (versionedPartitionName, livelyEndState) -> {
                                if (versionedPartitionName != null) {
                                    get(versionedPartitionName);
                                }
                                return null;
                            });
                            numOpened.incrementAndGet();
                        } catch (Throwable t) {
                            LOG.warn("Encountered the following opening partition:" + partitionName, t);
                            numFailed.incrementAndGet();
                        }
                    });
                } catch (Exception x) {
                    LOG.warn("Encountered the following getting properties for partition:" + partitionName, x);
                }
            }
            return true;
        });
        openExecutor.shutdown();
        while (!openExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            LOG.info("Still opening partitions: opened={} failed={} total={}", numOpened.get(), numFailed.get(), total.get());
        }
    }

    @Override
    public PartitionProperties getProperties(PartitionName partitionName) {

        return partitionProperties.computeIfAbsent(partitionName, (key) -> {
            try {
                if (partitionName.isSystemPartition()) {
                    return coldstartSystemPartitionProperties(partitionName);
                } else {
                    TimestampedValue rawPartitionProperties = getSystemPartition(PartitionCreator.REGION_PROPERTIES)
                        .getTimestampedValue(null, partitionName.toBytes());
                    if (rawPartitionProperties == null) {
                        return null;
                    }
                    return partitionPropertyMarshaller.fromBytes(rawPartitionProperties.getValue());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public PartitionStore get(VersionedPartitionName versionedPartitionName) throws Exception {
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        ConcurrentHashMap<Long, PartitionStore> versionedStores = partitionStores.get(partitionName);
        if (versionedStores != null) {
            PartitionStore partitionStore = versionedStores.get(versionedPartitionName.getPartitionVersion());
            if (partitionStore != null) {
                return partitionStore;
            }
        }

        if (!versionedPartitionName.getPartitionName().isSystemPartition()
            && !getSystemPartition(PartitionCreator.REGION_INDEX).containsKey(null, partitionName.toBytes())) {
            return null;
        }

        PartitionProperties properties = getProperties(partitionName);
        if (properties == null) {
            return null;
        }
        return open(versionedPartitionName, properties);
    }

    private PartitionStore getSystemPartition(VersionedPartitionName versionedPartitionName) {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Should only be called by system partitions.");
        ConcurrentHashMap<Long, PartitionStore> versionedPartitionStores = partitionStores.get(versionedPartitionName.getPartitionName());
        PartitionStore store = versionedPartitionStores == null ? null : versionedPartitionStores.get(0L);
        if (store == null) {
            throw new IllegalStateException("There is no system partition for " + versionedPartitionName);
        }
        return store;
    }

    public void delete(VersionedPartitionName versionedPartitionName) throws Exception {
        ConcurrentHashMap<Long, PartitionStore> versionedStores = partitionStores.get(versionedPartitionName.getPartitionName());
        if (versionedStores != null) {
            PartitionStore partitionStore = versionedStores.get(versionedPartitionName.getPartitionVersion());
            if (partitionStore != null) {
                partitionStore.delete();
                versionedStores.remove(versionedPartitionName.getPartitionVersion());
            }
        }
    }

    private PartitionStore open(VersionedPartitionName versionedPartitionName, PartitionProperties properties) throws Exception {
        synchronized (locksProvider.lock(versionedPartitionName, 1234)) {
            ConcurrentHashMap<Long, PartitionStore> versionedStores = partitionStores.computeIfAbsent(versionedPartitionName.getPartitionName(),
                (key) -> new ConcurrentHashMap<>());

            PartitionStore partitionStore = versionedStores.get(versionedPartitionName.getPartitionVersion());
            if (partitionStore != null) {
                return partitionStore;
            }

            WALStorage<?> walStorage = walStorageProvider.create(versionedPartitionName, properties.walStorageDescriptor);
            partitionStore = new PartitionStore(properties, walStorage, hardFlush);
            partitionStore.load();

            versionedStores.put(versionedPartitionName.getPartitionVersion(), partitionStore);
            LOG.info("Opened partition:" + versionedPartitionName);

            return partitionStore;
        }
    }

    public void putProperties(PartitionName partitionName, PartitionProperties properties) {
        partitionProperties.put(partitionName, properties);
    }

    public void removeProperties(PartitionName partitionName) {
        partitionProperties.remove(partitionName);
    }

    public boolean exists(VersionedPartitionName versionedPartitionName) throws Exception {
        return get(versionedPartitionName) != null;
    }

    @Override
    public Iterable<VersionedPartitionName> getAllPartitions() {
        return Iterables.concat(Iterables.transform(partitionStores.entrySet(), (partitionVersions) -> {
            return Iterables.transform(partitionVersions.getValue().keySet(), (partitionVersion) -> {
                return new VersionedPartitionName(partitionVersions.getKey(), partitionVersion);
            });
        }));
    }

    private PartitionProperties coldstartSystemPartitionProperties(PartitionName partitionName) {
        PartitionProperties properties;
        if (partitionName.equals(HIGHWATER_MARK_INDEX.getPartitionName())) {
            WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                false,
                new PrimaryIndexDescriptor("memory_persistent", 0, false, null),
                null,
                1000,
                1000);
            properties = new PartitionProperties(storageDescriptor, Consistency.none, true, 0, false, RowType.primary);
        } else if (partitionName.equals(AQUARIUM_LIVELINESS_INDEX.getPartitionName()) || partitionName.equals(AQUARIUM_STATE_INDEX.getPartitionName())) {
            WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                true,
                new PrimaryIndexDescriptor("memory_ephemeral", 0, false, null),
                null,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE);
            properties = new PartitionProperties(storageDescriptor, Consistency.none, true, 2, false, RowType.primary);
        } else {
            WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                false,
                new PrimaryIndexDescriptor("memory_persistent", 0, false, null),
                null,
                1000,
                1000);
            properties = new PartitionProperties(storageDescriptor, Consistency.none, true, 2, false, RowType.primary);
        }
        return properties;
    }

    // TODO this is never called
    @Override
    public void changes(final RowsChanged changes) throws Exception {
        if (changes.getVersionedPartitionName().getPartitionName().equals(REGION_PROPERTIES.getPartitionName())) {
            try {
                for (Map.Entry<WALKey, WALValue> entry : changes.getApply().entrySet()) {
                    PartitionName partitionName = PartitionName.fromBytes(entry.getKey().key);
                    removeProperties(partitionName);

                    ConcurrentHashMap<Long, PartitionStore> versionedPartitionStores = partitionStores.get(partitionName);
                    if (versionedPartitionStores != null) {
                        for (PartitionStore store : versionedPartitionStores.values()) {
                            PartitionProperties properties = getProperties(partitionName);
                            store.updateProperties(properties);
                        }
                    }
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Error while streaming entry set.", ex);
            }
        }
    }

}
