/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.storage;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.WALIndex;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.amza.shared.WALReader;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALTimestampId;
import com.jivesoftware.os.amza.shared.WALTx;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.WALWriter;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableLong;

public class IndexedWAL implements WALStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int numTickleMeElmaphore = 1024; // TODO config

    private final RegionName regionName;
    private final OrderIdProvider orderIdProvider;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final WALTx walTx;
    private final AtomicInteger maxUpdatesBetweenCompactionHintMarker;
    private final AtomicInteger maxUpdatesBetweenIndexCommitMarker;
    private final MutableLong[] stripedKeyHighwaterTimestamps;

    private final AtomicReference<WALIndex> walIndex = new AtomicReference<>(null);
    private final Object oneWriterAtATimeLock = new Object();
    private final Object oneTransactionAtATimeLock = new Object();
    private final Semaphore tickleMeElmophore = new Semaphore(numTickleMeElmaphore, true);
    private final AtomicLong updateCount = new AtomicLong(0);
    private final AtomicLong newCount = new AtomicLong(0);
    private final AtomicLong clobberCount = new AtomicLong(0);
    private final AtomicLong indexUpdates = new AtomicLong(0);

    public IndexedWAL(RegionName regionName,
        OrderIdProvider orderIdProvider,
        RowMarshaller<byte[]> rowMarshaller,
        WALTx walTx,
        int maxUpdatesBetweenCompactionHintMarker,
        int maxUpdatesBetweenIndexCommitMarker) {

        this.regionName = regionName;
        this.orderIdProvider = orderIdProvider;
        this.rowMarshaller = rowMarshaller;
        this.walTx = walTx;
        this.maxUpdatesBetweenCompactionHintMarker = new AtomicInteger(maxUpdatesBetweenCompactionHintMarker);
        this.maxUpdatesBetweenIndexCommitMarker = new AtomicInteger(maxUpdatesBetweenIndexCommitMarker);

        int numKeyHighwaterStripes = 1024; // TODO expose to config
        this.stripedKeyHighwaterTimestamps = new MutableLong[numKeyHighwaterStripes];
        for (int i = 0; i < stripedKeyHighwaterTimestamps.length; i++) {
            stripedKeyHighwaterTimestamps[i] = new MutableLong();
        }
    }

    public RegionName getRegionName() {
        return regionName;
    }

    @Override
    public long compactTombstone(long removeTombstonedOlderThanTimestampId, long ttlTimestampId) throws Exception {

        if ((clobberCount.get() + 1) / (newCount.get() + 1) > 2) { // TODO expose to config
            return compact(removeTombstonedOlderThanTimestampId, ttlTimestampId);
        }
        return -1;
    }

    private long compact(long removeTombstonedOlderThanTimestampId, long ttlTimestampId) throws Exception {
        final String metricPrefix = "region>" + regionName.getRegionName() + ">ring>" + regionName.getRingName() + ">";
        Optional<WALTx.Compacted> compact = walTx.compact(removeTombstonedOlderThanTimestampId, ttlTimestampId, walIndex.get());
        if (compact.isPresent()) {
            tickleMeElmophore.acquire(numTickleMeElmaphore);
            try {
                WALTx.CommittedCompacted compacted = compact.get().commit();
                walIndex.set(compacted.index);
                newCount.set(0);
                clobberCount.set(0);

                LOG.set(ValueType.COUNT, metricPrefix + "sizeBeforeCompaction", compacted.sizeBeforeCompaction);
                LOG.set(ValueType.COUNT, metricPrefix + "sizeAfterCompaction", compacted.sizeAfterCompaction);
                LOG.set(ValueType.COUNT, metricPrefix + "keeps", compacted.keyCount);
                LOG.set(ValueType.COUNT, metricPrefix + "removes", compacted.removeCount);
                LOG.set(ValueType.COUNT, metricPrefix + "tombstones", compacted.tombstoneCount);
                LOG.set(ValueType.COUNT, metricPrefix + "ttl", compacted.ttlCount);
                LOG.set(ValueType.COUNT, metricPrefix + "duration", compacted.duration);
                LOG.set(ValueType.COUNT, metricPrefix + "catchupKeeps", compacted.catchupKeys);
                LOG.set(ValueType.COUNT, metricPrefix + "catchupRemoves", compacted.catchupRemoves);
                LOG.set(ValueType.COUNT, metricPrefix + "catchupTombstones", compacted.catchupTombstones);
                LOG.set(ValueType.COUNT, metricPrefix + "catchupTtl", compacted.catchupTTL);
                LOG.set(ValueType.COUNT, metricPrefix + "catchupDuration", compacted.catchupDuration);
                LOG.inc(metricPrefix + "compacted");

                return compacted.sizeAfterCompaction;
            } finally {
                tickleMeElmophore.release(numTickleMeElmaphore);
            }

        } else {
            LOG.inc(metricPrefix + "checks");
        }
        return -1;
    }

    @Override
    public void load() throws Exception {
        tickleMeElmophore.acquire(numTickleMeElmaphore);
        try {

            walIndex.compareAndSet(null, walTx.load(regionName));

            final AtomicLong rowsVisited = new AtomicLong(maxUpdatesBetweenCompactionHintMarker.get());
            walTx.read(new WALTx.WALRead<Void>() {

                @Override
                public Void read(WALReader rowReader) throws Exception {
                    rowReader.reverseScan(new RowStream() {
                        @Override
                        public boolean row(long rowPointer, long rowTxId, byte rowType, byte[] row) throws Exception {
                            if (rowType == WALWriter.SYSTEM_VERSION_1) {
                                ByteBuffer buf = ByteBuffer.wrap(row);
                                byte[] keyBytes = new byte[8];
                                buf.get(keyBytes);
                                long key = FilerIO.bytesLong(keyBytes);
                                if (key == WALWriter.COMPACTION_HINTS_KEY) {
                                    byte[] newCountBytes = new byte[8];
                                    byte[] clobberCountBytes = new byte[8];
                                    buf.get(newCountBytes);
                                    buf.get(clobberCountBytes);
                                    newCount.set(FilerIO.bytesLong(newCountBytes));
                                    clobberCount.set(FilerIO.bytesLong(clobberCountBytes));
                                    return false;
                                }
                            }
                            return rowsVisited.decrementAndGet() >= 0;
                        }
                    });
                    return null;
                }
            });

            walTx.write(new WALTx.WALWrite<Void>() {

                @Override
                public Void write(WALWriter writer) throws Exception {
                    writeCompactionHintMarker(writer);
                    return null;
                }

            });

        } finally {
            tickleMeElmophore.release(numTickleMeElmaphore);
        }
    }

    @Override
    public void flush(boolean fsync) throws Exception {
        walTx.flush(fsync);
    }

    @Override
    public boolean delete(boolean ifEmpty) throws Exception {
        throw new UnsupportedOperationException("Not yet!");
    }

    private void writeCompactionHintMarker(WALWriter rowWriter) throws Exception {
        synchronized (oneTransactionAtATimeLock) {
            rowWriter.writeSystem(FilerIO.longsBytes(new long[] {
                WALWriter.COMPACTION_HINTS_KEY,
                newCount.get(),
                clobberCount.get()
            }));
            updateCount.set(0);
        }
    }

    private void writeIndexCommitMarker(WALWriter rowWriter, long indexCommitedUpToTxId) throws Exception {
        synchronized (oneTransactionAtATimeLock) {
            rowWriter.writeSystem(FilerIO.longsBytes(new long[] {
                WALWriter.COMMIT_KEY,
                indexCommitedUpToTxId }));
        }
    }

    /*TODO if we care, persist stripedKeyHighwaterTimestamps periodically as a cold start optimization for getLargestTimestampForKeyStripe()
    private void writeStripedTimestamp(WALWriter rowWriter) throws Exception {
        synchronized (oneTransactionAtATimeLock) {
            rowWriter.write(
                Collections.singletonList(-1L),
                Collections.singletonList(WALWriter.SYSTEM_VERSION_1),
                Collections.singletonList(FilerIO.longsBytes(new long[]{
                    WALWriter.STRIPE_TIME_MARKER,
                    stripedKeyHighwaterTimestamps})));
        }
    }
    */

    @Override
    public RowsChanged update(final boolean useUpdateTxId,
        WALReplicator walReplicator,
        WALStorageUpdateMode updateMode,
        Scannable<WALValue> updates) throws Exception {

        final AtomicLong oldestAppliedTimestamp = new AtomicLong(Long.MAX_VALUE);
        final Table<Long, WALKey, WALValue> apply = TreeBasedTable.create();
        final Map<WALKey, WALTimestampId> removes = new HashMap<>();
        final Map<WALKey, WALTimestampId> clobbers = new HashMap<>();

        final List<Long> transactionIds = useUpdateTxId ? new ArrayList<Long>() : null;
        final List<WALKey> keys = new ArrayList<>();
        final List<WALValue> values = new ArrayList<>();
        updates.rowScan(new Scan<WALValue>() {
            @Override
            public boolean row(long transactionId, WALKey key, WALValue update) throws Exception {
                if (useUpdateTxId) {
                    transactionIds.add(transactionId);
                }
                keys.add(key);
                values.add(update);
                return true;
            }
        });

        tickleMeElmophore.acquire();
        try {
            WALIndex wali = walIndex.get();
            RowsChanged rowsChanged;
            final MutableBoolean flushCompactionHint = new MutableBoolean(false);
            final MutableLong indexCommitedUpToTxId = new MutableLong();

            WALPointer[] currentPointers = wali.getPointers(keys.toArray(new WALKey[keys.size()]));
            for (int i = 0; i < keys.size(); i++) {
                WALKey key = keys.get(i);
                WALPointer currentPointer = currentPointers[i];
                WALValue update = values.get(i);
                if (currentPointer == null) {
                    apply.put(useUpdateTxId ? transactionIds.get(i) : -1, key, update);
                    if (oldestAppliedTimestamp.get() > update.getTimestampId()) {
                        oldestAppliedTimestamp.set(update.getTimestampId());
                    }
                } else if (currentPointer.getTimestampId() < update.getTimestampId()) {
                    apply.put(useUpdateTxId ? transactionIds.get(i) : -1, key, update);
                    if (oldestAppliedTimestamp.get() > update.getTimestampId()) {
                        oldestAppliedTimestamp.set(update.getTimestampId());
                    }
                    //TODO do we REALLY need the old value? WALValue value = hydrateRowIndexValue(currentPointer);
                    WALTimestampId currentTimestampId = new WALTimestampId(currentPointer.getTimestampId(), currentPointer.getTombstoned());
                    clobbers.put(key, currentTimestampId);
                    if (update.getTombstoned() && !currentPointer.getTombstoned()) {
                        removes.put(key, currentTimestampId);
                    }
                }
            }

            List<Future<?>> futures = Lists.newArrayListWithCapacity(1);

            if (walReplicator != null && updateMode == WALStorageUpdateMode.replicateThenUpdate && !apply.isEmpty()) {
                futures.add(walReplicator.replicate(new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers)));
            }

            final NavigableMap<WALKey, Long> keyToRowPointer = new TreeMap<>();

            if (apply.isEmpty()) {
                rowsChanged = new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers);
            } else {
                walTx.write(new WALTx.WALWrite<Void>() {
                    @Override
                    public Void write(WALWriter rowWriter) throws Exception {

                        List<Long> transactionIds = useUpdateTxId ? new ArrayList<Long>() : null;
                        List<WALKey> keys = new ArrayList<>();
                        List<byte[]> rawRows = new ArrayList<>();
                        for (Table.Cell<Long, WALKey, WALValue> cell : apply.cellSet()) {
                            if (useUpdateTxId) {
                                transactionIds.add(cell.getRowKey());
                            }
                            WALKey key = cell.getColumnKey();
                            WALValue value = cell.getValue();
                            keys.add(key);
                            rawRows.add(rowMarshaller.toRow(key, value));
                        }
                        long[] rowPointers;
                        synchronized (oneTransactionAtATimeLock) {
                            if (useUpdateTxId) {
                                indexCommitedUpToTxId.setValue(transactionIds.get(transactionIds.size() - 1));
                            } else {
                                long transactionId = orderIdProvider.nextId();
                                transactionIds = Collections.nCopies(rawRows.size(), transactionId);
                                indexCommitedUpToTxId.setValue(transactionId);
                            }
                            rowPointers = rowWriter.write(
                                transactionIds,
                                Collections.nCopies(rawRows.size(), WALWriter.VERSION_1),
                                rawRows);
                        }

                        for (int i = 0; i < rowPointers.length; i++) {
                            keyToRowPointer.put(keys.get(i), rowPointers[i]);
                        }

                        if (updateCount.addAndGet(keys.size()) > maxUpdatesBetweenCompactionHintMarker.get()) {
                            flushCompactionHint.setValue(true);
                        }
                        return null;
                    }
                });
                synchronized (oneWriterAtATimeLock) {
                    Iterator<Table.Cell<Long, WALKey, WALValue>> iter = apply.cellSet().iterator();
                    while (iter.hasNext()) {
                        Table.Cell<Long, WALKey, WALValue> cell = iter.next();
                        WALKey key = cell.getColumnKey();
                        WALValue value = cell.getValue();
                        long rowPointer = keyToRowPointer.get(key);
                        WALPointer pointer = new WALPointer(rowPointer, value.getTimestampId(), value.getTombstoned());
                        WALPointer got = wali.getPointer(key);
                        if (got == null) {
                            wali.put(Collections.singletonList(new SimpleEntry<>(key, pointer)));
                            newCount.incrementAndGet();
                        } else if (got.getTimestampId() < value.getTimestampId()) {
                            wali.put(Collections.singletonList(new SimpleEntry<>(key, pointer)));
                            clobberCount.incrementAndGet();
                        } else {
                            iter.remove();
                        }

                        int highwaterTimestampIndex = Math.abs(key.hashCode()) % stripedKeyHighwaterTimestamps.length;
                        stripedKeyHighwaterTimestamps[highwaterTimestampIndex].setValue(Math.max(
                            stripedKeyHighwaterTimestamps[highwaterTimestampIndex].longValue(),
                            value.getTimestampId()));
                    }
                    rowsChanged = new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers);
                }

                if (walReplicator != null && updateMode == WALStorageUpdateMode.updateThenReplicate && !apply.isEmpty()) {
                    futures.add(walReplicator.replicate(new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers)));
                }
            }

            final boolean commitAndWriteMarker = apply.size() > 0 && indexUpdates.addAndGet(apply.size()) > maxUpdatesBetweenIndexCommitMarker.get();
            if (commitAndWriteMarker) {
                wali.commit();
                indexUpdates.set(0);
            }
            if (commitAndWriteMarker || flushCompactionHint.isTrue()) {
                walTx.write(new WALTx.WALWrite<Void>() {
                    @Override
                    public Void write(WALWriter writer) throws Exception {
                        if (flushCompactionHint.isTrue()) {
                            writeCompactionHintMarker(writer);
                        }
                        if (commitAndWriteMarker) {
                            writeIndexCommitMarker(writer, indexCommitedUpToTxId.longValue());
                        }
                        return null;
                    }

                });
            }
            for (Future<?> future : futures) {
                future.get();
            }
            return rowsChanged;
        } finally {
            tickleMeElmophore.release();
        }
    }

    @Override
    public void rowScan(final Scan<WALValue> scan) throws Exception {
        tickleMeElmophore.acquire();
        try {
            WALIndex wali = walIndex.get();
            wali.rowScan(new Scan<WALPointer>() {
                @Override
                public boolean row(long transactionId, WALKey key, WALPointer rowPointer) throws Exception {
                    return scan.row(transactionId, key, hydrateRowIndexValue(rowPointer));
                }
            });
        } finally {
            tickleMeElmophore.release();
        }
    }

    @Override
    public void rangeScan(final WALKey from, final WALKey to, final Scan<WALValue> scan) throws Exception {
        tickleMeElmophore.acquire();
        try {
            WALIndex wali = walIndex.get();
            wali.rangeScan(from, to, new Scan<WALPointer>() {
                @Override
                public boolean row(long transactionId, WALKey key, WALPointer rowPointer) throws Exception {
                    return scan.row(transactionId, key, hydrateRowIndexValue(rowPointer));
                }
            });
        } finally {
            tickleMeElmophore.release();
        }
    }

    @Override
    public WALValue get(WALKey key) throws Exception {
        tickleMeElmophore.acquire();
        try {
            WALPointer got = walIndex.get().getPointer(key);
            if (got != null) {
                return hydrateRowIndexValue(got);
            }
            return null;
        } finally {
            tickleMeElmophore.release();
        }
    }

    @Override
    public WALValue[] get(WALKey[] keys) throws Exception {
        tickleMeElmophore.acquire();
        try {
            WALPointer[] gots = walIndex.get().getPointers(keys);
            WALValue[] values = new WALValue[gots.length];
            for (int i = 0; i < values.length; i++) {
                if (gots[i] != null) {
                    values[i] = hydrateRowIndexValue(gots[i]);
                }
            }
            return values;
        } finally {
            tickleMeElmophore.release();
        }
    }

    @Override
    public WALPointer[] getPointers(WALKey[] consumableKeys, List<WALValue> values) throws Exception {
        tickleMeElmophore.acquire();
        try {
            for (int i = 0; i < consumableKeys.length; i++) {
                WALKey key = consumableKeys[i];
                if (key != null) {
                    WALValue value = values.get(i);
                    if (value.getTimestampId() > getLargestTimestampForKeyStripe(key)) {
                        consumableKeys[i] = null;
                        LOG.inc("getPointers>excluded");
                    }
                    LOG.inc("getPointers>total");
                }
            }
            return walIndex.get().getPointers(consumableKeys);
        } finally {
            tickleMeElmophore.release();
        }
    }

    @Override
    public boolean containsKey(WALKey key) throws Exception {
        return containsKey(Collections.singletonList(key)).get(0);
    }

    @Override
    public List<Boolean> containsKey(List<WALKey> keys) throws Exception {
        tickleMeElmophore.acquire();
        try {
            return walIndex.get().containsKey(keys);
        } finally {
            tickleMeElmophore.release();
        }
    }

    private long getLargestTimestampForKeyStripe(WALKey key) {
        int highwaterTimestampIndex = Math.abs(key.hashCode()) % stripedKeyHighwaterTimestamps.length;
        return stripedKeyHighwaterTimestamps[highwaterTimestampIndex].longValue();
    }

    private WALValue hydrateRowIndexValue(final WALPointer indexFP) {
        try {
            byte[] row = walTx.read(new WALTx.WALRead<byte[]>() {
                @Override
                public byte[] read(WALReader rowReader) throws Exception {
                    return rowReader.read(indexFP.getFp());
                }
            });
            byte[] value = rowMarshaller.valueFromRow(row);
            return new WALValue(value,
                indexFP.getTimestampId(),
                indexFP.getTombstoned());
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrtate " + indexFP, x);
        }
    }

    @Override
    public boolean takeRowUpdatesSince(final long sinceTransactionId, final RowStream rowStream) throws Exception {
        tickleMeElmophore.acquire();
        try {
            return walTx.readFromTransactionId(sinceTransactionId, new WALTx.WALReadWithOffset<Boolean>() {

                @Override
                public Boolean read(long offset, WALReader rowReader) throws Exception {
                    return rowReader.scan(offset, false, new RowStream() {
                        @Override
                        public boolean row(long rowPointer, long rowTxId, byte rowType, byte[] row) throws Exception {
                            if (rowType > 0 && rowTxId > sinceTransactionId) {
                                return rowStream.row(rowPointer, rowTxId, rowType, row);
                            }
                            return true;
                        }
                    });
                }
            });
        } finally {
            tickleMeElmophore.release();
        }
    }

    @Override
    public boolean takeFromTransactionId(final long sinceTransactionId, final Scan<WALValue> scan) throws Exception {
        tickleMeElmophore.acquire();
        try {
            return walTx.readFromTransactionId(sinceTransactionId, new WALTx.WALReadWithOffset<Boolean>() {

                @Override
                public Boolean read(long offset, WALReader rowReader) throws Exception {
                    return rowReader.scan(offset, false, new RowStream() {
                        @Override
                        public boolean row(long rowPointer, long rowTxId, byte rowType, byte[] row) throws Exception {
                            if (rowType > 0 && rowTxId > sinceTransactionId) {
                                WALRow walRow = rowMarshaller.fromRow(row);
                                return scan.row(rowTxId, walRow.getKey(), walRow.getValue());
                            }
                            return true;
                        }
                    });
                }
            });
        } finally {
            tickleMeElmophore.release();
        }
    }

    @Override
    public void updatedStorageDescriptor(WALStorageDescriptor walStorageDescriptor) throws Exception {
        maxUpdatesBetweenCompactionHintMarker.set(walStorageDescriptor.maxUpdatesBetweenCompactionHintMarker);
        maxUpdatesBetweenIndexCommitMarker.set(walStorageDescriptor.maxUpdatesBetweenIndexCommitMarker);
        tickleMeElmophore.acquire();
        try {
            WALIndex wali = walIndex.get();
            wali.updatedDescriptors(walStorageDescriptor.primaryIndexDescriptor, walStorageDescriptor.secondaryIndexDescriptors);
        } finally {
            tickleMeElmophore.release();
        }
    }

    @Override
    public long count() throws Exception {
        tickleMeElmophore.acquire();
        try {
            WALIndex wali = this.walIndex.get();
            return wali.size();
        } finally {
            tickleMeElmophore.release();
        }
    }
}
