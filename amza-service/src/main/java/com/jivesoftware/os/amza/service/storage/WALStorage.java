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
package com.jivesoftware.os.amza.service.storage;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.RangeScannable;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.wal.KeyContainedStream;
import com.jivesoftware.os.amza.shared.wal.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.KeyValues;
import com.jivesoftware.os.amza.shared.wal.KeyedTimestampId;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.TxKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALIndexable;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALKeyValuePointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeys;
import com.jivesoftware.os.amza.shared.wal.WALMergeKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.WALReader;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.wal.WALTimestampId;
import com.jivesoftware.os.amza.shared.wal.WALTx;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.amza.shared.wal.WALWriter;
import com.jivesoftware.os.amza.shared.wal.WALWriter.IndexableKeys;
import com.jivesoftware.os.amza.shared.wal.WALWriter.RawRows;
import com.jivesoftware.os.amza.shared.wal.WALWriter.TxKeyPointerFpStream;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

public class WALStorage<I extends WALIndex> implements RangeScannable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int numTickleMeElmaphore = 1024; // TODO config
    private static final int numKeyHighwaterStripes = 1024; // TODO expose to config

    private final VersionedPartitionName versionedPartitionName;
    private final OrderIdProvider orderIdProvider;
    private final PrimaryRowMarshaller<byte[]> primaryRowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    private final WALTx<I> walTx;
    private final AtomicInteger maxUpdatesBetweenCompactionHintMarker;
    private final AtomicInteger maxUpdatesBetweenIndexCommitMarker;
    private final long[] stripedKeyHighwaterTimestamps;

    private final AtomicReference<I> walIndex = new AtomicReference<>(null);
    private final Object oneIndexerAtATimeLock = new Object();
    private final Object oneTransactionAtATimeLock = new Object();
    private final Semaphore tickleMeElmophore = new Semaphore(numTickleMeElmaphore, true);
    private final AtomicLong updateCount = new AtomicLong(0);
    private final AtomicLong keyCount = new AtomicLong(0);
    private final AtomicLong clobberCount = new AtomicLong(0);
    private final AtomicLong indexUpdates = new AtomicLong(0);
    private final AtomicLong highestTxId = new AtomicLong(0);
    private final int tombstoneCompactionFactor;

    private final ThreadLocal<Integer> reentrant = new ReentrantTheadLocal();

    static class ReentrantTheadLocal extends ThreadLocal<Integer> {

        @Override
        protected Integer initialValue() {
            return 0;
        }
    }

    public WALStorage(VersionedPartitionName versionedPartitionName,
        OrderIdProvider orderIdProvider,
        PrimaryRowMarshaller<byte[]> rowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller,
        WALTx<I> walTx,
        int maxUpdatesBetweenCompactionHintMarker,
        int maxUpdatesBetweenIndexCommitMarker,
        int tombstoneCompactionFactor) {

        this.versionedPartitionName = versionedPartitionName;
        this.orderIdProvider = orderIdProvider;
        this.primaryRowMarshaller = rowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.walTx = walTx;
        this.maxUpdatesBetweenCompactionHintMarker = new AtomicInteger(maxUpdatesBetweenCompactionHintMarker);
        this.maxUpdatesBetweenIndexCommitMarker = new AtomicInteger(maxUpdatesBetweenIndexCommitMarker);
        this.tombstoneCompactionFactor = tombstoneCompactionFactor;
        this.stripedKeyHighwaterTimestamps = new long[numKeyHighwaterStripes];
    }

    private void acquireOne() {
        try {
            int enters = reentrant.get();
            if (enters == 0) {
                tickleMeElmophore.acquire();
            }
            reentrant.set(enters + 1);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void releaseOne() {
        int enters = reentrant.get();
        if (enters - 1 == 0) {
            tickleMeElmophore.release();
        }
        reentrant.set(enters - 1);
    }

    private void acquireAll() throws InterruptedException {
        tickleMeElmophore.acquire(numTickleMeElmaphore);
    }

    private void releaseAll() {
        tickleMeElmophore.release(numTickleMeElmaphore);
    }

    public VersionedPartitionName getVersionedPartitionName() {
        return versionedPartitionName;
    }

    public boolean expunge() throws Exception {
        boolean expunged = true;
        acquireAll();
        try {
            expunged &= walTx.delete(false);
            I wali = walIndex.get();
            if (wali != null) {
                expunged &= wali.delete();
            }
        } finally {
            releaseAll();
        }
        return expunged;
    }

    public boolean compactableTombstone(long removeTombstonedOlderTimestampId, long ttlTimestampId) throws Exception {
        return (clobberCount.get() + 1) / (keyCount.get() + 1) > tombstoneCompactionFactor;
    }

    public long compactTombstone(long removeTombstonedOlderThanTimestampId, long ttlTimestampId, boolean force) throws Exception {
        final String metricPrefix = "partition>" + new String(versionedPartitionName.getPartitionName().getName())
            + ">ring>" + new String(versionedPartitionName.getPartitionName().getRingName()) + ">";
        Optional<WALTx.Compacted<I>> compact = walTx.compact(removeTombstonedOlderThanTimestampId, ttlTimestampId, walIndex.get(), force);
        if (compact.isPresent()) {
            acquireAll();
            try {
                WALTx.CommittedCompacted<I> compacted = compact.get().commit();
                walIndex.set(compacted.index);
                keyCount.set(compacted.keyCount + compacted.catchupKeyCount);
                clobberCount.set(0);
                walTx.write((WALWriter writer) -> {

                    writeCompactionHintMarker(writer);
                    writeIndexCommitMarker(writer, highestTxId()); // Kevin said this would be ok!
                    return null;
                });

                LOG.set(ValueType.COUNT, metricPrefix + "sizeBeforeCompaction", compacted.sizeBeforeCompaction);
                LOG.set(ValueType.COUNT, metricPrefix + "sizeAfterCompaction", compacted.sizeAfterCompaction);
                LOG.set(ValueType.COUNT, metricPrefix + "keeps", compacted.keyCount);
                LOG.set(ValueType.COUNT, metricPrefix + "clobbers", compacted.clobberCount);
                LOG.set(ValueType.COUNT, metricPrefix + "tombstones", compacted.tombstoneCount);
                LOG.set(ValueType.COUNT, metricPrefix + "ttl", compacted.ttlCount);
                LOG.set(ValueType.COUNT, metricPrefix + "duration", compacted.duration);
                LOG.set(ValueType.COUNT, metricPrefix + "catchupKeeps", compacted.catchupKeyCount);
                LOG.set(ValueType.COUNT, metricPrefix + "catchupClobbers", compacted.catchupClobberCount);
                LOG.set(ValueType.COUNT, metricPrefix + "catchupTombstones", compacted.catchupTombstoneCount);
                LOG.set(ValueType.COUNT, metricPrefix + "catchupTtl", compacted.catchupTTLCount);
                LOG.set(ValueType.COUNT, metricPrefix + "catchupDuration", compacted.catchupDuration);
                LOG.inc(metricPrefix + "compacted");

                return compacted.sizeAfterCompaction;
            } finally {
                releaseAll();
            }

        } else {
            LOG.inc(metricPrefix + "checks");
        }
        return -1;
    }

    public void load() throws Exception {
        acquireAll();
        try {

            walIndex.compareAndSet(null, walTx.load(versionedPartitionName));

            MutableLong lastTxId = new MutableLong(0);
            MutableLong rowsVisited = new MutableLong(maxUpdatesBetweenCompactionHintMarker.get());
            MutableBoolean needCompactionHints = new MutableBoolean(true);
            MutableBoolean needKeyHighwaterStripes = new MutableBoolean(true);
            MutableBoolean needHighestTxId = new MutableBoolean(true);

            KeyValueStream mergeStripedKeyHighwaters = (byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) -> {
                mergeStripedKeyHighwaters(key, valueTimestamp);
                return true;
            };

            walTx.read((WALReader rowReader) -> {
                rowReader.reverseScan((rowPointer, rowTxId, rowType, row) -> {
                    if (rowTxId > lastTxId.longValue()) {
                        lastTxId.setValue(rowTxId);
                        needHighestTxId.setValue(false);
                    }
                    if ((needCompactionHints.booleanValue() || needKeyHighwaterStripes.booleanValue()) && rowType == RowType.system) {
                        long key = UIO.bytesLong(row, 0);
                        if (key == RowType.COMPACTION_HINTS_KEY) {
                            long[] parts = UIO.bytesLongs(row);
                            if (needCompactionHints.booleanValue()) {
                                keyCount.set(parts[1]);
                                clobberCount.set(parts[2]);
                                needCompactionHints.setValue(false);
                            }
                            if (needKeyHighwaterStripes.booleanValue()) {
                                if (parts.length - 3 == numKeyHighwaterStripes) {
                                    for (int i = 0; i < numKeyHighwaterStripes; i++) {
                                        stripedKeyHighwaterTimestamps[i] = Math.max(stripedKeyHighwaterTimestamps[i], parts[i + 3]);
                                    }
                                    needKeyHighwaterStripes.setValue(false);
                                }
                            }
                        }
                    }
                    if (needKeyHighwaterStripes.booleanValue() && rowType == RowType.primary) {
                        primaryRowMarshaller.fromRow(row, mergeStripedKeyHighwaters);
                    }
                    rowsVisited.decrement();
                    return needHighestTxId.booleanValue() || needCompactionHints.booleanValue() || needKeyHighwaterStripes.booleanValue();
                });
                return null;
            });
            if (!highestTxId.compareAndSet(0, lastTxId.longValue())) {
                throw new RuntimeException("Load should have completed before highestTxId:" + highestTxId.get() + " is modified.");
            }
            LOG.info("Loaded partition:{} with a highestTxId:{}", versionedPartitionName, lastTxId.longValue());

            walTx.write((WALWriter writer) -> {
                writeCompactionHintMarker(writer);
                return null;
            });

        } finally {
            releaseAll();
        }
    }

    private void mergeStripedKeyHighwaters(byte[] key, long valueTimestamp) {
        int highwaterTimestampIndex = Math.abs(Arrays.hashCode(key) % stripedKeyHighwaterTimestamps.length);
        stripedKeyHighwaterTimestamps[highwaterTimestampIndex] = Math.max(stripedKeyHighwaterTimestamps[highwaterTimestampIndex], valueTimestamp);
    }

    public void flush(boolean fsync) throws Exception {
        walTx.flush(fsync);
    }

    public void commitIndex() throws Exception {
        WALIndex wali = walIndex.get();
        if (wali != null) {
            wali.commit();
        } else {
            LOG.warn("Trying to commit a nonexistent index:{}.", versionedPartitionName);
        }
    }

    public boolean delete(boolean ifEmpty) throws Exception {
        throw new UnsupportedOperationException("Not yet!");
    }

    private void writeCompactionHintMarker(WALWriter rowWriter) throws Exception {
        synchronized (oneTransactionAtATimeLock) {
            long[] hints = new long[3 + numKeyHighwaterStripes];
            hints[0] = RowType.COMPACTION_HINTS_KEY;
            hints[1] = keyCount.get();
            hints[2] = clobberCount.get();
            System.arraycopy(stripedKeyHighwaterTimestamps, 0, hints, 3, numKeyHighwaterStripes);

            rowWriter.writeSystem(UIO.longsBytes(hints));
            updateCount.set(0);
        }
    }

    private void writeIndexCommitMarker(WALWriter rowWriter, long indexCommitedUpToTxId) throws Exception {
        synchronized (oneTransactionAtATimeLock) {
            rowWriter.writeSystem(UIO.longsBytes(new long[]{
                RowType.COMMIT_KEY,
                indexCommitedUpToTxId}));
        }
    }

    public void writeHighwaterMarker(WALWriter rowWriter, WALHighwater highwater) throws Exception {
        synchronized (oneTransactionAtATimeLock) {
            rowWriter.writeHighwater(highwaterRowMarshaller.toBytes(highwater));
        }
    }

    public RowsChanged update(long forceTxId, boolean forceApply, Commitable updates) throws Exception {

        AtomicLong oldestAppliedTimestamp = new AtomicLong(Long.MAX_VALUE);
        Map<WALKey, WALValue> apply = new LinkedHashMap<>();
        List<KeyedTimestampId> removes = new ArrayList<>();
        List<KeyedTimestampId> clobbers = new ArrayList<>();

        List<byte[]> keys = new ArrayList<>();
        List<WALValue> values = new ArrayList<>();
        WALHighwater[] highwater = new WALHighwater[1];

        updates.commitable((_highwater) -> {
            highwater[0] = _highwater;

        }, (transactionId, key, value, valueTimestamp, valueTombstoned) -> {
            WALValue walValue = new WALValue(value, valueTimestamp, valueTombstoned);
            if (!forceApply) {
                keys.add(key);
                values.add(walValue);
            } else {
                apply.put(new WALKey(key), walValue);
            }
            return true;
        });

        acquireOne();
        try {
            WALIndex wali = walIndex.get();
            RowsChanged rowsChanged;
            MutableBoolean flushCompactionHint = new MutableBoolean(false);
            MutableLong indexCommitedUpToTxId = new MutableLong();

            if (!forceApply) {
                MutableInt i = new MutableInt(0);
                wali.getPointers((KeyValueStream stream) -> {
                    for (int k = 0; k < keys.size(); k++) {
                        WALValue value = values.get(k);
                        if (!stream.stream(keys.get(k), value.getValue(), value.getTimestampId(), value.getTombstoned())) {
                            return false;
                        }
                    }
                    return true;
                }, (key, value, valueTimestamp, valueTombstone, pointerTimestamp, pointerTombstoned, pointerFp) -> {
                    WALKey walKey = new WALKey(key);
                    WALValue walValue = new WALValue(value, valueTimestamp, valueTombstone);
                    if (pointerFp == -1) {
                        apply.put(walKey, walValue);
                        if (oldestAppliedTimestamp.get() > valueTimestamp) {
                            oldestAppliedTimestamp.set(valueTimestamp);
                        }
                    } else if (pointerTimestamp < valueTimestamp) {
                        apply.put(walKey, walValue);
                        if (oldestAppliedTimestamp.get() > valueTimestamp) {
                            oldestAppliedTimestamp.set(valueTimestamp);
                        }
                        //TODO do we REALLY need the old value? WALValue value = hydrateRowIndexValue(currentPointer);
                        WALTimestampId currentTimestampId = new WALTimestampId(pointerTimestamp, pointerTombstoned);
                        KeyedTimestampId keyedTimestampId = new KeyedTimestampId(walKey.getKey(),
                            currentTimestampId.getTimestampId(),
                            currentTimestampId.getTombstoned());
                        clobbers.add(keyedTimestampId);
                        if (valueTombstone && !pointerTombstoned) {
                            removes.add(keyedTimestampId);
                        }
                    }
                    i.increment();
                    return true;
                });
            }

            if (apply.isEmpty()) {
                rowsChanged = new RowsChanged(versionedPartitionName, oldestAppliedTimestamp.get(), apply, removes, clobbers, -1);
            } else {
                List<WALIndexable> indexables = new ArrayList<>(apply.size());
                walTx.write((WALWriter rowWriter) -> {
                    flush(forceTxId,
                        rowStream -> {
                            for (Entry<WALKey, WALValue> row : apply.entrySet()) {
                                WALValue value = row.getValue();
                                if (!rowStream.stream(primaryRowMarshaller.toRow(row.getKey().getKey(),
                                        value.getValue(),
                                        value.getTimestampId(),
                                        value.getTombstoned()))) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        indexableKeyStream -> {
                            for (Entry<WALKey, WALValue> row : apply.entrySet()) {
                                WALValue value = row.getValue();
                                if (!indexableKeyStream.stream(row.getKey().getKey(), value.getTimestampId(), value.getTombstoned())) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        indexCommitedUpToTxId,
                        rowWriter,
                        highwater[0],
                        flushCompactionHint,
                        (rowTxId, key, valueTimestamp, valueTombstoned, fp) -> {
                            indexables.add(new WALIndexable(rowTxId, key, valueTimestamp, valueTombstoned, fp));
                            return true;
                        });

                    return null;
                });
                synchronized (oneIndexerAtATimeLock) {
                    wali.merge((TxKeyPointerStream stream) -> {
                        for (WALIndexable indexable : indexables) {
                            if (!stream.stream(indexable.txId, indexable.key, indexable.valueTimestamp, indexable.valueTombstoned, indexable.fp)) {
                                return false;
                            }

                            mergeStripedKeyHighwaters(indexable.key, indexable.valueTimestamp);

                        }
                        return true;
                    }, (mode, txId, key, timestamp, tombstoned, fp) -> {
                        if (mode == WALMergeKeyPointerStream.added) {
                            keyCount.incrementAndGet();
                        } else if (mode == WALMergeKeyPointerStream.clobbered) {
                            clobberCount.incrementAndGet();
                        } else {
                            apply.remove(new WALKey(key));
                        }
                        return true;
                    });

                    rowsChanged = new RowsChanged(versionedPartitionName,
                        oldestAppliedTimestamp.get(),
                        apply,
                        removes,
                        clobbers,
                        indexCommitedUpToTxId.longValue());
                }
            }

            final boolean commitAndWriteMarker = apply.size() > 0 && indexUpdates.addAndGet(apply.size()) > maxUpdatesBetweenIndexCommitMarker.get();
            if (commitAndWriteMarker) {
                wali.commit();
                indexUpdates.set(0);
            }
            if (commitAndWriteMarker || flushCompactionHint.isTrue()) {
                walTx.write((WALWriter writer) -> {
                    if (flushCompactionHint.isTrue()) {
                        writeCompactionHintMarker(writer);
                    }
                    if (commitAndWriteMarker) {
                        writeIndexCommitMarker(writer, indexCommitedUpToTxId.longValue());
                    }
                    return null;
                });
            }
            return rowsChanged;
        } finally {
            releaseOne();
        }
    }

    private void flush(long txId,
        RawRows rows,
        IndexableKeys indexableKeys,
        MutableLong indexCommitedUpToTxId,
        WALWriter rowWriter,
        WALHighwater highwater,
        MutableBoolean flushCompactionHint,
        TxKeyPointerFpStream stream) throws Exception {

        int count;
        synchronized (oneTransactionAtATimeLock) {
            if (txId == -1) {
                txId = orderIdProvider.nextId();
            }
            indexCommitedUpToTxId.setValue(txId);
            count = rowWriter.write(txId, RowType.primary, rows, indexableKeys, stream);
            if (highwater != null) {
                writeHighwaterMarker(rowWriter, highwater);
            }
            highestTxId.set(indexCommitedUpToTxId.longValue());
        }
        if (updateCount.addAndGet(count) > maxUpdatesBetweenCompactionHintMarker.get()) {
            flushCompactionHint.setValue(true);
        }
    }

    @Override
    public boolean rowScan(KeyValueStream keyValueStream) throws Exception {
        acquireOne();
        try {
            WALIndex wali = walIndex.get();
            return wali.rowScan((key, timestamp, tombstoned, fp) -> {
                return keyValueStream.stream(key, hydrateRowIndexValue(fp), timestamp, tombstoned);
            });
        } finally {
            releaseOne();
        }
    }

    @Override
    public boolean rangeScan(byte[] from, byte[] to, KeyValueStream keyValueStream) throws Exception {
        acquireOne();
        try {
            WALIndex wali = walIndex.get();
            return wali.rangeScan(from,
                to,
                (key, timestamp, tombstoned, fp) -> keyValueStream.stream(key, hydrateRowIndexValue(fp), timestamp, tombstoned));
        } finally {
            releaseOne();
        }
    }

    // TODO fix barf
    public TimestampedValue get(byte[] key) throws Exception {
        acquireOne();
        try {
            TimestampedValue[] values = new TimestampedValue[1];
            walIndex.get().getPointer(key, (key1, timestamp, tombstoned, fp) -> {
                if (fp != -1 && !tombstoned) {
                    values[0] = new TimestampedValue(timestamp, hydrateRowIndexValue(fp));
                }
                return true;
            });
            return values[0];
        } finally {
            releaseOne();
        }
    }

    public boolean get(WALKeys keys, KeyValueStream keyValueStream) throws Exception {
        acquireOne();
        try {
            return walIndex.get().getPointers(keys,
                (key, pointerTimestamp, pointerTombstoned, pointerFp) -> {
                    if (pointerFp != -1 && !pointerTombstoned) {
                        return keyValueStream.stream(key, hydrateRowIndexValue(pointerFp), pointerTimestamp, pointerTombstoned);
                    } else {
                        return keyValueStream.stream(key, null, -1, false);
                    }
                });
        } finally {
            releaseOne();
        }
    }

    public boolean streamWALPointers(KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception {
        acquireOne();
        try {
            return walIndex.get().getPointers(
                indexStream -> keyValues.consume(
                    (key, value, valueTimestamp, valueTombstone) -> {
                        if (valueTimestamp > getLargestTimestampForKeyStripe(key)) {
                            return stream.stream(key, value, valueTimestamp, valueTombstone, -1, false, -1);
                        } else {
                            return indexStream.stream(key, value, valueTimestamp, valueTombstone);
                        }
                    }),
                stream);
        } finally {
            releaseOne();
        }
    }

    public boolean containsKeys(WALKeys keys, KeyContainedStream stream) throws Exception {
        acquireOne();
        try {
            return walIndex.get().containsKeys(keys, stream);
        } finally {
            releaseOne();
        }
    }

    // TODO fix instanciation to hashcode
    private long getLargestTimestampForKeyStripe(byte[] key) {
        int highwaterTimestampIndex = Math.abs(Arrays.hashCode(key) % stripedKeyHighwaterTimestamps.length);
        return stripedKeyHighwaterTimestamps[highwaterTimestampIndex];
    }

    private byte[] hydrateRowIndexValue(long indexFP) {
        try {
            byte[] row = walTx.read((WALReader rowReader) -> rowReader.read(indexFP));
            return primaryRowMarshaller.valueFromRow(row);
        } catch (Exception x) {
            String base64;
            try {
                base64 = versionedPartitionName.toBase64();
            } catch (IOException e) {
                base64 = e.getMessage();
            }

            long length;
            try {
                length = walTx.length();
            } catch (Exception e) {
                length = -1;
            }

            throw new RuntimeException("Failed to hydrate for " + versionedPartitionName + " base64=" + base64 + " size=" + length + " fp=" + indexFP, x);
        }
    }

    public boolean takeRowUpdatesSince(final long sinceTransactionId, final RowStream rowStream) throws Exception {
        if (sinceTransactionId >= highestTxId.get()) {
            return true;
        }
        acquireOne();
        try {
            long[] takeMetrics = new long[1];
            Boolean readFromTransactionId = walTx.readFromTransactionId(sinceTransactionId, (long offset, WALReader rowReader) -> rowReader.scan(offset, false,
                (long rowPointer, long rowTxId, RowType rowType, byte[] row) -> {
                    if (rowType != RowType.system && rowTxId > sinceTransactionId) {
                        return rowStream.row(rowPointer, rowTxId, rowType, row);
                    } else {
                        takeMetrics[0]++;
                    }
                    return true;
                }));
            LOG.inc("excessTakes", takeMetrics[0]);
            return readFromTransactionId;
        } finally {
            releaseOne();
        }
    }

    public boolean takeAllRows(final RowStream rowStream) throws Exception {
        acquireOne();
        try {
            return walTx.readFromTransactionId(0, (long offset, WALReader rowReader) -> rowReader.scan(offset, false,
                (long rowPointer, long rowTxId, RowType rowType, byte[] row) -> {
                    return rowStream.row(rowPointer, rowTxId, rowType, row);
                }));
        } finally {
            releaseOne();
        }
    }

    public void updatedStorageDescriptor(WALStorageDescriptor walStorageDescriptor) throws Exception {
        maxUpdatesBetweenCompactionHintMarker.set(walStorageDescriptor.maxUpdatesBetweenCompactionHintMarker);
        maxUpdatesBetweenIndexCommitMarker.set(walStorageDescriptor.maxUpdatesBetweenIndexCommitMarker);
        acquireOne();
        try {
            WALIndex wali = walIndex.get();
            wali.updatedDescriptors(walStorageDescriptor.primaryIndexDescriptor, walStorageDescriptor.secondaryIndexDescriptors);
        } finally {
            releaseOne();
        }
    }

    public long count() throws Exception {
        return keyCount.get();
    }

    public long highestTxId() {
        return highestTxId.get();
    }
}
