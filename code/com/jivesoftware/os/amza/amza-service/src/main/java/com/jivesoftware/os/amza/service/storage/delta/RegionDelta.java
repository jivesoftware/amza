package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALTimestampId;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.WALRow;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
class RegionDelta {

    public static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RegionName regionName;
    private final DeltaWAL deltaWAL;
    private final RowMarshaller<byte[]> rowMarshaller;
    final AtomicReference<RegionDelta> compacting;

    private final Map<WALKey, WALPointer> pointerIndex = new ConcurrentHashMap<>();
    private final ConcurrentNavigableMap<WALKey, WALPointer> orderedIndex = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<Long, List<Long>> txIdWAL = new ConcurrentSkipListMap<>();

    RegionDelta(RegionName regionName, DeltaWAL deltaWAL, RowMarshaller<byte[]> rowMarshaller, RegionDelta compacting) {
        this.regionName = regionName;
        this.deltaWAL = deltaWAL;
        this.rowMarshaller = rowMarshaller;
        this.compacting = new AtomicReference<>(compacting);
    }

    WALValue get(WALKey key) throws Exception {
        WALPointer got = pointerIndex.get(key);
        if (got == null) {
            RegionDelta regionDelta = compacting.get();
            if (regionDelta != null) {
                return regionDelta.get(key);
            }
            return null;
        }
        return deltaWAL.hydrate(got.getFp()).getValue();
    }

    WALTimestampId getTimestampId(WALKey key) throws Exception {
        WALPointer got = pointerIndex.get(key);
        if (got != null) {
            return new WALTimestampId(got.getTimestampId(), got.getTombstoned());
        }
        RegionDelta regionDelta = compacting.get();
        if (regionDelta != null) {
            return regionDelta.getTimestampId(key);
        }
        return null;
    }

    DeltaResult<WALTimestampId[]> getTimestampIds(WALKey[] consumableKeys) throws Exception {
        boolean missed = false;
        WALTimestampId[] result = new WALTimestampId[consumableKeys.length];
        for (int i = 0; i < consumableKeys.length; i++) {
            WALKey key = consumableKeys[i];
            if (key != null) {
                WALTimestampId got = getTimestampId(key);
                if (got != null) {
                    result[i] = got;
                    consumableKeys[i] = null;
                } else {
                    missed = true;
                }
            }
        }
        return new DeltaResult<>(missed, result);
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
        WALPointer got = pointerIndex.get(key);
        if (got != null) {
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

    void put(WALKey key, WALPointer rowPointer) {
        pointerIndex.put(key, rowPointer);
        orderedIndex.put(key, rowPointer);
    }

    Set<WALKey> keySet() {
        Set<WALKey> keySet = pointerIndex.keySet();
        RegionDelta regionDelta = compacting.get();
        if (regionDelta != null) {
            HashSet<WALKey> all = new HashSet<>(keySet);
            all.addAll(regionDelta.keySet());
            return all;
        }
        return keySet;
    }

    DeltaPeekableElmoIterator rangeScanIterator(WALKey from, WALKey to) {
        Iterator<Map.Entry<WALKey, WALPointer>> iterator = orderedIndex.subMap(from, to).entrySet().iterator();
        Iterator<Map.Entry<WALKey, WALPointer>> compactingIterator = Iterators.emptyIterator();
        RegionDelta compactingRegionDelta = compacting.get();
        DeltaWAL compactingDeltaWAL = null;
        if (compactingRegionDelta != null) {
            compactingIterator = compactingRegionDelta.orderedIndex.subMap(from, to).entrySet().iterator();
            compactingDeltaWAL = compactingRegionDelta.deltaWAL;
        }
        return new DeltaPeekableElmoIterator(iterator, compactingIterator, deltaWAL, compactingDeltaWAL);
    }

    DeltaPeekableElmoIterator rowScanIterator() {
        Iterator<Map.Entry<WALKey, WALPointer>> iterator = orderedIndex.entrySet().iterator();
        Iterator<Map.Entry<WALKey, WALPointer>> compactingIterator = Iterators.emptyIterator();
        RegionDelta compactingRegionDelta = compacting.get();
        DeltaWAL compactingDeltaWAL = null;
        if (compactingRegionDelta != null) {
            compactingIterator = compactingRegionDelta.orderedIndex.entrySet().iterator();
            compactingDeltaWAL = compactingRegionDelta.deltaWAL;
        }
        return new DeltaPeekableElmoIterator(iterator, compactingIterator, deltaWAL, compactingDeltaWAL);
    }

    void appendTxFps(long rowTxId, long rowFP) {
        List<Long> fps = txIdWAL.get(rowTxId);
        if (fps == null) {
            fps = new ArrayList<>();
            txIdWAL.put(rowTxId, fps);
        }
        fps.add(rowFP);
    }

    void appendTxFps(long rowTxId, Collection<Long> rowFPs) {
        List<Long> fps = txIdWAL.get(rowTxId);
        if (fps != null) {
            throw new IllegalStateException("Already appended this txId: " + rowTxId);
        }
        txIdWAL.put(rowTxId, ImmutableList.copyOf(rowFPs));
    }

    boolean takeRowUpdatesSince(long transactionId, RowStream rowStream) throws Exception {
        ConcurrentNavigableMap<Long, List<Long>> tailMap = txIdWAL.tailMap(transactionId, false);
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

    public boolean takeFromTransactionId(final long transactionId, final Scan<WALValue> scan) throws Exception {
        RegionDelta regionDelta = compacting.get();
        if (regionDelta != null) {
            if (!regionDelta.takeFromTransactionId(transactionId, scan)) {
                return false;
            }
        }

        if (txIdWAL.isEmpty() || txIdWAL.lastEntry().getKey() < transactionId) {
            return true;
        }

        ConcurrentNavigableMap<Long, List<Long>> tailMap = txIdWAL.tailMap(transactionId, false);
        return deltaWAL.takeRows(tailMap, new RowStream() {
            @Override
            public boolean row(long rowFP, long rowTxId, byte rowType, byte[] row) throws Exception {
                if (rowType > 0) {
                    WALRow walRow = rowMarshaller.fromRow(row);
                    return scan.row(rowTxId, walRow.getKey(), walRow.getValue());
                }
                return true;
            }
        });
    }

    void compact(RegionIndex regionIndex) throws Exception {
        final RegionDelta compact = compacting.get();
        if (compact != null) {
            if (!compact.txIdWAL.isEmpty()) {
                LOG.info("Merging (" + compact.orderedIndex.size() + ") deltas for " + compact.regionName);
                RegionStore regionStore = regionIndex.get(compact.regionName);
                regionStore.directCommit(true,
                    null,
                    WALStorageUpdateMode.noReplication,
                    new Scannable<WALValue>() {
                        @Override
                        public void rowScan(Scan<WALValue> scan) {
                            try {
                                eos:
                                for (Map.Entry<Long, List<Long>> e : compact.txIdWAL.entrySet()) {
                                    long txId = e.getKey();
                                    for (long fp : e.getValue()) {
                                        WALRow walRow = compact.deltaWAL.hydrate(fp);
                                        WALKey key = walRow.getKey();
                                        WALValue value = walRow.getValue();
                                        if (compact.orderedIndex.get(key).getFp() == fp) {
                                            if (!scan.row(txId, key, value)) {
                                                break eos;
                                            }
                                        }
                                    }
                                }
                            } catch (Throwable ex) {
                                throw new RuntimeException("Error while streaming entry set.", ex);
                            }
                        }
                    });
                LOG.info("Merged deltas for " + compact.regionName);
            }
        }
        compacting.set(null);
    }

}
