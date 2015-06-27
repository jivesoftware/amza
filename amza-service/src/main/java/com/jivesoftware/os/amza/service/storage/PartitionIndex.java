package com.jivesoftware.os.amza.service.storage;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALStorage;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.wal.WALStorageProvider;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jivesoftware.os.amza.service.storage.PartitionProvider.HIGHWATER_MARK_INDEX;
import static com.jivesoftware.os.amza.service.storage.PartitionProvider.REGION_PROPERTIES;

/**
 * @author jonathan.colt
 */
public class PartitionIndex implements RowChanges, VersionedPartitionProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ConcurrentHashMap<PartitionName, ConcurrentHashMap<Long, PartitionStore>> partitionStores = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PartitionName, PartitionProperties> partitionProperties = new ConcurrentHashMap<>();
    private final StripingLocksProvider<VersionedPartitionName> locksProvider = new StripingLocksProvider<>(1024); // TODO expose to config

    private final AmzaStats amzaStats;
    private final String[] workingDirectories;
    private final String domain;
    private final WALStorageProvider walStorageProvider;
    private final PartitionPropertyMarshaller partitionPropertyMarshaller;
    private final boolean hardFlush;

    public PartitionIndex(AmzaStats amzaStats,
        String[] workingDirectories,
        String domain,
        WALStorageProvider walStorageProvider,
        PartitionPropertyMarshaller partitionPropertyMarshaller,
        boolean hardFlush) {

        this.amzaStats = amzaStats;
        this.workingDirectories = workingDirectories;
        this.domain = domain;
        this.walStorageProvider = walStorageProvider;
        this.partitionPropertyMarshaller = partitionPropertyMarshaller;
        this.hardFlush = hardFlush;
    }

    public void open(TxPartitionStatus txPartitionState) throws Exception {

        PartitionStore partitionIndexStore = get(PartitionProvider.REGION_INDEX);
        final ExecutorService openExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        final AtomicInteger numOpened = new AtomicInteger(0);
        final AtomicInteger numFailed = new AtomicInteger(0);
        final AtomicInteger total = new AtomicInteger(0);
        partitionIndexStore.rowScan((long rowTxId, WALKey key, WALValue value) -> {
            final PartitionName partitionName = PartitionName.fromBytes(key.getKey());
            try {
                total.incrementAndGet();
                openExecutor.submit(() -> {
                    try {
                        txPartitionState.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
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
            return true;
        });
        openExecutor.shutdown();
        while (!openExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            LOG.info("Still opening partitions: opened={} failed={} total={}", numOpened.get(), numFailed.get(), total.get());
        }

        get(PartitionProvider.RING_INDEX);
        get(PartitionProvider.NODE_INDEX);
        get(PartitionProvider.HIGHWATER_MARK_INDEX);
        get(PartitionProvider.REGION_ONLINE_INDEX);
        get(PartitionProvider.REGION_PROPERTIES);
    }

    public PartitionProperties getProperties(PartitionName partitionName) {

        PartitionProperties properties = partitionProperties.get(partitionName);
        if (properties == null && partitionName.isSystemPartition()) {
            properties = coldstartSystemPartitionProperties(partitionName);
            partitionProperties.put(partitionName, properties);
        }
        return properties;
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

        WALKey partitionNameKey = new WALKey(partitionName.toBytes());
        if (!versionedPartitionName.getPartitionName().isSystemPartition()) {
            if (!getSystemPartition(PartitionProvider.REGION_INDEX).containsKey(partitionNameKey)) {
                return null;
            }
        }

        PartitionProperties properties = getProperties(partitionName);
        if (properties == null) {
            WALValue rawPartitionProperties = getSystemPartition(PartitionProvider.REGION_PROPERTIES).get(partitionNameKey);
            if (rawPartitionProperties == null || rawPartitionProperties.getTombstoned()) {
                return null;
            }
            properties = partitionPropertyMarshaller.fromBytes(rawPartitionProperties.getValue());
            partitionProperties.put(partitionName, properties);
        }
        return open(versionedPartitionName, properties);
    }

    private PartitionStore getSystemPartition(VersionedPartitionName versionedPartitionName) {
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition(), "Should ony be called by system partitions.");
        ConcurrentHashMap<Long, PartitionStore> versionedPartitionStores = partitionStores.get(versionedPartitionName.getPartitionName());
        PartitionStore store = versionedPartitionStores == null ? null : versionedPartitionStores.get(0L);
        if (store == null) {
            throw new IllegalStateException("There is no system partition for " + versionedPartitionName);
        }
        return store;
    }

    public PartitionStore remove(VersionedPartitionName versionedPartitionName) {
        ConcurrentHashMap<Long, PartitionStore> versionedStores = partitionStores.get(versionedPartitionName.getPartitionName());
        if (versionedStores != null) {
            return versionedStores.remove(versionedPartitionName.getPartitionVersion());
        }
        return null;
    }

    private PartitionStore open(VersionedPartitionName versionedPartitionName, PartitionProperties properties) throws Exception {
        synchronized (locksProvider.lock(versionedPartitionName, 1234)) {
            ConcurrentHashMap<Long, PartitionStore> versionedStores = partitionStores.computeIfAbsent(versionedPartitionName.getPartitionName(),
                (key) -> new ConcurrentHashMap<>());

            PartitionStore partitionStore = versionedStores.get(versionedPartitionName.getPartitionVersion());
            if (partitionStore != null) {
                return partitionStore;
            }

            File workingDirectory = new File(workingDirectories[Math.abs(versionedPartitionName.hashCode()) % workingDirectories.length]);
            WALStorage walStorage = walStorageProvider.create(workingDirectory, domain, versionedPartitionName, properties.walStorageDescriptor);
            partitionStore = new PartitionStore(walStorage, hardFlush);
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
                new PrimaryIndexDescriptor("memory", 0, false, null), null, 1000, 1000);
            properties = new PartitionProperties(storageDescriptor, 0, 0, false);
        } else {
            WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                new PrimaryIndexDescriptor("memory", 0, false, null), null, 1000, 1000);
            properties = new PartitionProperties(storageDescriptor, 2, 2, false);
        }
        return properties;
    }

    // TODO this is never called
    @Override
    public void changes(final RowsChanged changes) throws Exception {
        if (changes.getVersionedPartitionName().getPartitionName().equals(REGION_PROPERTIES.getPartitionName())) {
            changes.commitable(null, (long rowTxId, WALKey key, WALValue scanned) -> {
                PartitionName partitionName = PartitionName.fromBytes(key.getKey());
                removeProperties(partitionName);

                ConcurrentHashMap<Long, PartitionStore> versionedPartitionStores = partitionStores.get(partitionName);
                if (versionedPartitionStores != null) {
                    for (PartitionStore store : versionedPartitionStores.values()) {
                        PartitionProperties properties = getProperties(partitionName);
                        store.updatedStorageDescriptor(properties.walStorageDescriptor);
                    }
                }
                return true;
            });
        }
    }

}
