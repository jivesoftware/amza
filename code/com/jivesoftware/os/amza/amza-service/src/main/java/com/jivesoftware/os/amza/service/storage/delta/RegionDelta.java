package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Iterators;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author jonathan.colt
 */
class RegionDelta {

    public static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RegionName regionName;
    private final DeltaWAL deltaWAL;
    private final ConcurrentNavigableMap<WALKey, WALValue> index = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<Long, Collection<byte[]>> txIdWAL = new ConcurrentSkipListMap<>();
    final AtomicReference<RegionDelta> compacting;

    RegionDelta(RegionName regionName, DeltaWAL deltaWAL, RegionDelta compacting) {
        this.regionName = regionName;
        this.deltaWAL = deltaWAL;
        this.compacting = new AtomicReference<>(compacting);
    }

    WALValue get(WALKey key) throws Exception {
        WALValue got = index.get(key);
        if (got == null) {
            RegionDelta regionDelta = compacting.get();
            if (regionDelta != null) {
                return regionDelta.get(key);
            }
            return null;
        }
        return deltaWAL.hydrate(regionName, got);
    }

    DeltaResult<List<WALValue>> get(List<WALKey> keys) throws Exception {
        boolean missed = false;
        List<WALValue> result = new ArrayList<>(keys.size());
        for (WALKey key : keys) {
            WALValue got = get(key);
            missed |= (got == null);
            result.add(got);
        }
        return new DeltaResult<>(missed, result);
    }

    boolean containsKey(WALKey key) {
        if (index.get(key) != null) {
            return true;
        }
        RegionDelta regionDelta = compacting.get();
        if (regionDelta != null) {
            return regionDelta.containsKey(key);
        }
        return false;
    }

    DeltaResult<List<Boolean>> containsKey(List<WALKey> keys) {
        boolean missed = false;
        List<Boolean> result = new ArrayList<>(keys.size());
        for (WALKey key : keys) {
            boolean got = containsKey(key);
            missed |= got;
            result.add(got);
        }
        return new DeltaResult<>(missed, result);
    }

    void put(WALKey key, WALValue rowValue) {
        index.put(key, rowValue);
    }

    Set<WALKey> keySet() {
        Set<WALKey> keySet = index.keySet();
        RegionDelta regionDelta = compacting.get();
        if (regionDelta != null) {
            HashSet<WALKey> all = new HashSet<>(keySet);
            all.addAll(regionDelta.keySet());
            return all;
        }
        return keySet;
    }

    DeltaPeekableElmoIterator rangeScanIterator(WALKey from, WALKey to) {
        Iterator<Map.Entry<WALKey, WALValue>> iterator = index.subMap(from, to).entrySet().iterator();
        Iterator<Map.Entry<WALKey, WALValue>> compactingIterator = Iterators.emptyIterator();
        RegionDelta regionDelta = compacting.get();
        if (regionDelta != null) {
            compactingIterator = regionDelta.index.subMap(from, to).entrySet().iterator();
        }
        return new DeltaPeekableElmoIterator(iterator, compactingIterator);
    }

    DeltaPeekableElmoIterator rowScanIterator() {
        Iterator<Map.Entry<WALKey, WALValue>> iterator = index.entrySet().iterator();
        Iterator<Map.Entry<WALKey, WALValue>> compactingIterator = Iterators.emptyIterator();
        RegionDelta regionDelta = compacting.get();
        if (regionDelta != null) {
            compactingIterator = regionDelta.index.entrySet().iterator();
        }
        return new DeltaPeekableElmoIterator(iterator, compactingIterator);
    }

    void appendTxFps(long rowTxId, long rowFP) {
        Collection<byte[]> fps = txIdWAL.get(rowTxId);
        if (fps == null) {
            fps = new ArrayList<>();
            txIdWAL.put(rowTxId, fps);
        }
        fps.add(UIO.longBytes(rowFP));
    }

    void appendTxFps(long rowTxId, Collection<byte[]> rowFPs) {
        Collection<byte[]> fps = txIdWAL.get(rowTxId);
        if (fps == null) {
            fps = new ArrayList<>();
            txIdWAL.put(rowTxId, fps);
        }
        fps.addAll(rowFPs);
    }

    boolean takeRowUpdatesSince(long transactionId, RowStream rowStream) throws Exception {
        ConcurrentNavigableMap<Long, Collection<byte[]>> tailMap = txIdWAL.tailMap(transactionId);
        deltaWAL.takeRows(tailMap, rowStream);
        if (!txIdWAL.isEmpty() && txIdWAL.firstEntry().getKey() <= transactionId) {
            return true;
        }
        RegionDelta regionDelta = compacting.get();
        if (regionDelta != null) {
            if (regionDelta.takeRowUpdatesSince(transactionId, rowStream)) {
                return true;
            }
        }
        return false;
    }

    long compact(RegionProvider regionProvider) throws Exception {
        final RegionDelta compact = compacting.get();
        long largestTxId = 0;
        if (compact != null) {
            LOG.info("Merging deltas for " + compact.regionName);
            largestTxId = compact.txIdWAL.lastKey();
            RegionStore regionStore = regionProvider.getRegionStore(compact.regionName);
            regionStore.directCommit(largestTxId,
                new WALScanable() {
                    @Override
                    public void rowScan(WALScan walScan) {
                        for (Map.Entry<WALKey, WALValue> e : compact.index.entrySet()) {
                            try {
                                if (!walScan.row(-1, e.getKey(), compact.deltaWAL.hydrate(compact.regionName, e.getValue()))) {
                                    break;
                                }
                            } catch (Throwable ex) {
                                throw new RuntimeException("Error while streaming entry set.", ex);
                            }
                        }
                    }
                });
            LOG.info("Merged deltas for " + compact.regionName);

        }
        compacting.set(null);
        return largestTxId;
    }

}
