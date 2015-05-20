package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
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
import static com.jivesoftware.os.amza.service.storage.RegionProvider.RING_INDEX;

/**
 *
 * @author jonathan.colt
 */
public class RegionIndex implements RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ConcurrentHashMap<RegionName, RegionStore> regionStores = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RegionName, RegionProperties> regionProperties = new ConcurrentHashMap<>();
    private final StripingLocksProvider locksProvider = new StripingLocksProvider(1024); // TODO expose to config

    private final AmzaStats amzaStats;
    private final String[] workingDirectories;
    private final String domain;
    private final WALStorageProvider walStorageProvider;
    private final RegionPropertyMarshaller regionPropertyMarshaller;
    private final boolean hardFlush;

    public RegionIndex(AmzaStats amzaStats, String[] workingDirectories, String domain, WALStorageProvider walStorageProvider,
        RegionPropertyMarshaller regionPropertyMarshaller, boolean hardFlush) {
        this.amzaStats = amzaStats;
        this.workingDirectories = workingDirectories;
        this.domain = domain;
        this.walStorageProvider = walStorageProvider;
        this.regionPropertyMarshaller = regionPropertyMarshaller;
        this.hardFlush = hardFlush;
    }

    public void open() throws Exception {

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
                        get(regionName);
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

        get(RING_INDEX);
        get(HIGHWATER_MARK_INDEX);
        get(REGION_PROPERTIES);
    }

    public RegionProperties getProperties(RegionName regionName) {

        RegionProperties properties = regionProperties.get(regionName);
        if (properties == null && regionName.isSystemRegion()) {
            properties = coldstartSystemRegionProperties(regionName);
            regionProperties.put(regionName, properties);
        }
        return properties;
    }

    public RegionStore get(RegionName regionName) throws Exception {
        RegionStore regionStore = regionStores.get(regionName);
        if (regionStore != null) {
            return regionStore;
        }
        RegionProperties properties = getProperties(regionName);
        if (properties == null) {
            RegionStore store = regionStores.get(RegionProvider.REGION_PROPERTIES);
            if (store != null) {
                WALValue rawRegionProperties = store.get(new WALKey(regionName.toBytes()));
                if (rawRegionProperties == null || rawRegionProperties.getTombstoned()) {
                    return null;
                }
                properties = regionPropertyMarshaller.fromBytes(rawRegionProperties.getValue());
                regionProperties.put(regionName, properties);
            } else {
                return null;
            }
        }
        return open(regionName, properties);
    }

    private RegionStore open(RegionName regionName, RegionProperties properties) throws Exception {
        synchronized (locksProvider.lock(regionName, 1234)) {
            RegionStore regionStore = regionStores.get(regionName);
            if (regionStore != null) {
                return regionStore;
            }

            File workingDirectory = new File(workingDirectories[Math.abs(regionName.hashCode()) % workingDirectories.length]);
            WALStorage walStorage = walStorageProvider.create(workingDirectory, domain, regionName, properties.walStorageDescriptor);
            regionStore = new RegionStore(walStorage, hardFlush);
            regionStore.load();

            regionStores.put(regionName, regionStore);
            LOG.info("Opened region:" + regionName);
            return regionStore;
        }
    }

    public void putProperties(RegionName regionName, RegionProperties properties) {
        regionProperties.put(regionName, properties);
    }

    public void put(RegionName regionName, RegionStore regionStore) {
        regionStores.put(regionName, regionStore);
    }

    public void removeProperties(RegionName regionName) {
        regionProperties.remove(regionName);
    }

    public boolean exists(RegionName regionName) throws Exception {
        return get(regionName) != null;
    }

    public Iterable<RegionName> getActiveRegions() {
        return regionStores.keySet();
    }

    private RegionProperties coldstartSystemRegionProperties(RegionName regionName) {
        RegionProperties properties;
        if (regionName.equals(HIGHWATER_MARK_INDEX)) {
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
        if (changes.getRegionName().equals(REGION_PROPERTIES)) {
            changes.rowScan((long rowTxId, WALKey key, WALValue scanned) -> {
                removeProperties(RegionName.fromBytes(key.getKey()));
                RegionStore store = get(changes.getRegionName());
                if (store != null) {
                    RegionProperties properties = getProperties(changes.getRegionName());
                    store.updatedStorageDescriptor(properties.walStorageDescriptor);
                }
                return true;
            });
        }
    }
}
