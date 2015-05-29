package com.jivesoftware.os.amza.service.storage;

import com.google.common.collect.Iterables;
import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.TxRegionStatus;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALStorageProvider;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jivesoftware.os.amza.service.storage.RegionProvider.HIGHWATER_MARK_INDEX;
import static com.jivesoftware.os.amza.service.storage.RegionProvider.REGION_PROPERTIES;

/**
 *
 * @author jonathan.colt
 */
public class RegionIndex implements RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ConcurrentHashMap<RegionName, ConcurrentHashMap<Long, RegionStore>> regionStores = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RegionName, RegionProperties> regionProperties = new ConcurrentHashMap<>();
    private final StripingLocksProvider locksProvider = new StripingLocksProvider(1024); // TODO expose to config

    private final AmzaStats amzaStats;
    private final String[] workingDirectories;
    private final String domain;
    private final WALStorageProvider walStorageProvider;
    private final RegionPropertyMarshaller regionPropertyMarshaller;
    private final boolean hardFlush;

    public RegionIndex(AmzaStats amzaStats,
        String[] workingDirectories,
        String domain,
        WALStorageProvider walStorageProvider,
        RegionPropertyMarshaller regionPropertyMarshaller,
        boolean hardFlush) {

        this.amzaStats = amzaStats;
        this.workingDirectories = workingDirectories;
        this.domain = domain;
        this.walStorageProvider = walStorageProvider;
        this.regionPropertyMarshaller = regionPropertyMarshaller;
        this.hardFlush = hardFlush;
    }

    public void open(TxRegionStatus txRegionState) throws Exception {

        RegionStore regionIndexStore = get(RegionProvider.REGION_INDEX);
        final ExecutorService openExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        final AtomicInteger numOpened = new AtomicInteger(0);
        final AtomicInteger numFailed = new AtomicInteger(0);
        final AtomicInteger total = new AtomicInteger(0);
        regionIndexStore.rowScan((long rowTxId, WALKey key, WALValue value) -> {
            final RegionName regionName = RegionName.fromBytes(key.getKey());
            try {
                total.incrementAndGet();
                openExecutor.submit(() -> {
                    try {
                        txRegionState.tx(regionName, (versionedRegionName, regionStatus) -> {
                            get(versionedRegionName);
                            return null;
                        });
                        numOpened.incrementAndGet();
                    } catch (Throwable t) {
                        LOG.warn("Encountered the following opening region:" + regionName, t);
                        numFailed.incrementAndGet();
                    }
                });
            } catch (Exception x) {
                LOG.warn("Encountered the following getting properties for region:" + regionName, x);
            }
            return true;
        });
        openExecutor.shutdown();
        while (!openExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            LOG.info("Still opening regions: opened={} failed={} total={}", numOpened.get(), numFailed.get(), total.get());
        }

        get(RegionProvider.RING_INDEX);
        get(RegionProvider.NODE_INDEX);
        get(RegionProvider.HIGHWATER_MARK_INDEX);
        get(RegionProvider.REGION_ONLINE_INDEX);
        get(RegionProvider.REGION_PROPERTIES);
    }

    public RegionProperties getProperties(RegionName regionName) {

        RegionProperties properties = regionProperties.get(regionName);
        if (properties == null && regionName.isSystemRegion()) {
            properties = coldstartSystemRegionProperties(regionName);
            regionProperties.put(regionName, properties);
        }
        return properties;
    }

    public RegionStore get(VersionedRegionName versionedRegionName) throws Exception {
        ConcurrentHashMap<Long, RegionStore> versionedStores = regionStores.get(versionedRegionName.getRegionName());
        if (versionedStores != null) {
            RegionStore regionStore = versionedStores.get(versionedRegionName.getRegionVersion());
            if (regionStore != null) {
                return regionStore;
            }
        }

        RegionProperties properties = getProperties(versionedRegionName.getRegionName());
        if (properties == null) {
            ConcurrentHashMap<Long, RegionStore> versionedRegionPropertiesStores = regionStores.get(RegionProvider.REGION_PROPERTIES);
            RegionStore store = versionedRegionPropertiesStores == null ? null : versionedRegionPropertiesStores.get(0L);
            if (store != null) {
                WALValue rawRegionProperties = store.get(new WALKey(versionedRegionName.toBytes()));
                if (rawRegionProperties == null || rawRegionProperties.getTombstoned()) {
                    return null;
                }
                properties = regionPropertyMarshaller.fromBytes(rawRegionProperties.getValue());
                regionProperties.put(versionedRegionName.getRegionName(), properties);
            } else {
                return null;
            }
        }
        return open(versionedRegionName, properties);
    }



    public RegionStore remove(VersionedRegionName versionedRegionName) {
        ConcurrentHashMap<Long, RegionStore> versionedStores = regionStores.get(versionedRegionName.getRegionName());
        if (versionedStores != null) {
            return versionedStores.remove(versionedRegionName.getRegionVersion());
        }
        return null;
    }

    private RegionStore open(VersionedRegionName versionedRegionName, RegionProperties properties) throws Exception {
        synchronized (locksProvider.lock(versionedRegionName, 1234)) {
            ConcurrentHashMap<Long, RegionStore> versionedStores = regionStores.computeIfAbsent(versionedRegionName.getRegionName(),
                (regionName) -> new ConcurrentHashMap<>());

            RegionStore regionStore = versionedStores.get(versionedRegionName.getRegionVersion());
            if (regionStore != null) {
                return regionStore;
            }

            File workingDirectory = new File(workingDirectories[Math.abs(versionedRegionName.hashCode()) % workingDirectories.length]);
            WALStorage walStorage = walStorageProvider.create(workingDirectory, domain, versionedRegionName, properties.walStorageDescriptor);
            regionStore = new RegionStore(walStorage, hardFlush);
            regionStore.load();

            versionedStores.put(versionedRegionName.getRegionVersion(), regionStore);
            LOG.info("Opened region:" + versionedRegionName);
            return regionStore;
        }
    }

    public void put(VersionedRegionName versionedRegionName, RegionStore regionStore) {
        ConcurrentHashMap<Long, RegionStore> versionedStores = regionStores.computeIfAbsent(versionedRegionName.getRegionName(),
            (regionName) -> new ConcurrentHashMap<>());
        versionedStores.put(versionedRegionName.getRegionVersion(), regionStore);
    }

    public void putProperties(RegionName regionName, RegionProperties properties) {
        regionProperties.put(regionName, properties);
    }

    public void removeProperties(RegionName regionName) {
        regionProperties.remove(regionName);
    }

    public boolean exists(VersionedRegionName versionedRegionName) throws Exception {
        return get(versionedRegionName) != null;
    }

    public Iterable<VersionedRegionName> getAllRegions() {
        return Iterables.concat(Iterables.transform(regionStores.entrySet(), (regionVersions) -> {
            return Iterables.transform(regionVersions.getValue().keySet(), (regionVersion) -> {
                return new VersionedRegionName(regionVersions.getKey(), regionVersion);
            });
        }));
    }
   
    private RegionProperties coldstartSystemRegionProperties(RegionName regionName) {
        RegionProperties properties;
        if (regionName.equals(HIGHWATER_MARK_INDEX.getRegionName())) {
            WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                new PrimaryIndexDescriptor("memory", 0, false, null), null, 1000, 1000);
            properties = new RegionProperties(storageDescriptor, 0, 0, false);
        } else {
            WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                new PrimaryIndexDescriptor("memory", 0, false, null), null, 1000, 1000);
            properties = new RegionProperties(storageDescriptor, 2, 2, false);
        }
        return properties;
    }

    // TODO this is never called
    @Override
    public void changes(final RowsChanged changes) throws Exception {
        if (changes.getVersionedRegionName().getRegionName().equals(REGION_PROPERTIES.getRegionName())) {
            changes.commitable(null, (long rowTxId, WALKey key, WALValue scanned) -> {
                RegionName regionName = RegionName.fromBytes(key.getKey());
                removeProperties(regionName);

                ConcurrentHashMap<Long, RegionStore> versionedRegionStores = regionStores.get(regionName);
                if (versionedRegionStores != null) {
                    for (RegionStore store : versionedRegionStores.values()) {
                        RegionProperties properties = getProperties(regionName);
                        store.updatedStorageDescriptor(properties.walStorageDescriptor);
                    }
                }
                return true;
            });
        }
    }

}
