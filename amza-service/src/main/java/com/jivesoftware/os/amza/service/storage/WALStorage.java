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
import com.jivesoftware.os.amza.api.CompareTimestampVersions;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.partition.WALStorageDescriptor;
import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.scan.RangeScannable;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.FpKeyValueStream;
import com.jivesoftware.os.amza.api.stream.KeyContainedStream;
import com.jivesoftware.os.amza.api.stream.KeyValuePointerStream;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.KeyValues;
import com.jivesoftware.os.amza.api.stream.TxKeyPointerStream;
import com.jivesoftware.os.amza.api.stream.WALKeyPointers;
import com.jivesoftware.os.amza.api.stream.WALMergeKeyPointerStream;
import com.jivesoftware.os.amza.api.wal.KeyedTimestampId;
import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.WALIndex;
import com.jivesoftware.os.amza.api.wal.WALIndexable;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALReader;
import com.jivesoftware.os.amza.api.wal.WALTimestampId;
import com.jivesoftware.os.amza.api.wal.WALTx;
import com.jivesoftware.os.amza.api.wal.WALValue;
import com.jivesoftware.os.amza.api.wal.WALWriter;
import com.jivesoftware.os.amza.api.wal.WALWriter.IndexableKeys;
import com.jivesoftware.os.amza.api.wal.WALWriter.RawRows;
import com.jivesoftware.os.amza.api.wal.WALWriter.TxKeyPointerFpStream;
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
    private final PrimaryRowMarshaller primaryRowMarshaller;
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
    private final AtomicLong highestTxId = new AtomicLong(-1);
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
        PrimaryRowMarshaller rowMarshaller,
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

    public long compactTombstone(RowType rowType, long removeTombstonedOlderThanTimestampId, long ttlTimestampId, boolean force) throws Exception {
        final String metricPrefix = "partition>" + new String(versionedPartitionName.getPartitionName().getName())
            + ">ring>" + new String(versionedPartitionName.getPartitionName().getRingName()) + ">";
        Optional<WALTx.Compacted<I>> compact = walTx.compact(rowType, removeTombstonedOlderThanTimestampId, ttlTimestampId, walIndex.get(), force);
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

            walIndex.compareAndSet(null, walTx.load(versionedPartitionName, maxUpdatesBetweenCompactionHintMarker.get()));

            MutableLong lastTxId = new MutableLong(-1);
            MutableLong rowsVisited = new MutableLong(maxUpdatesBetweenCompactionHintMarker.get());
            MutableBoolean needCompactionHints = new MutableBoolean(true);
            MutableBoolean needKeyHighwaterStripes = new MutableBoolean(true);
            MutableBoolean needHighestTxId = new MutableBoolean(true);

            FpKeyValueStream mergeStripedKeyHighwaters = (fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                mergeStripedKeyHighwaters(prefix, key, valueTimestamp);
                return true;
            };

            MutableLong keys = new MutableLong(0);
            MutableLong clobbers = new MutableLong(0);
            boolean completed = primaryRowMarshaller.fromRows(fpRowStream -> {
                return walTx.read(rowReader -> {
                    rowReader.reverseScan((rowFP, rowTxId, rowType, row) -> {
                        if (rowTxId > lastTxId.longValue()) {
                            lastTxId.setValue(rowTxId);
                            needHighestTxId.setValue(false);
                        }
                        if ((needCompactionHints.booleanValue() || needKeyHighwaterStripes.booleanValue()) && rowType == RowType.system) {
                            long key = UIO.bytesLong(row, 0);
                            if (key == RowType.COMPACTION_HINTS_KEY) {
                                long[] parts = UIO.bytesLongs(row);
                                if (needCompactionHints.booleanValue()) {
                                    keys.add(parts[1]);
                                    clobbers.add(parts[2]);
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
                        if (rowType.isPrimary()) {
                            if (needCompactionHints.booleanValue()) {
                                keys.increment();
                            }
                            if (needKeyHighwaterStripes.booleanValue() && !fpRowStream.stream(rowFP, rowType, row)) {
                                return false;
                            }
                        }
                        rowsVisited.decrement();
                        return needHighestTxId.booleanValue() || needCompactionHints.booleanValue() || needKeyHighwaterStripes.booleanValue();
                    });
                    return true;
                });
            }, mergeStripedKeyHighwaters);

            if (!completed) {
                throw new IllegalStateException("WALStorage failed to load completely for: " + versionedPartitionName);
            }

            keyCount.set(keys.longValue());
            clobberCount.set(clobbers.longValue());

            if (!highestTxId.compareAndSet(-1, lastTxId.longValue())) {
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

    private void mergeStripedKeyHighwaters(byte[] prefix, byte[] key, long valueTimestamp) {
        int highwaterTimestampIndex = Math.abs((Arrays.hashCode(prefix) ^ Arrays.hashCode(key)) % stripedKeyHighwaterTimestamps.length);
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

    public RowsChanged update(RowType rowType, long forceTxId, boolean forceApply, byte[] prefix, Commitable updates) throws Exception {
        if (!rowType.isPrimary()) {
            throw new IllegalArgumentException("rowType:" + rowType + " needs to be of type primary.");
        }

        Map<WALKey, WALValue> apply = new LinkedHashMap<>();
        List<KeyedTimestampId> removes = new ArrayList<>();
        List<KeyedTimestampId> clobbers = new ArrayList<>();

        List<byte[]> keys = new ArrayList<>();
        List<WALValue> values = new ArrayList<>();
        WALHighwater[] highwater = new WALHighwater[1];
        long updateVersion = orderIdProvider.nextId();

        updates.commitable((_highwater) -> {
            highwater[0] = _highwater;

        }, (transactionId, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
            WALValue walValue = new WALValue(rowType, value, valueTimestamp, valueTombstoned, valueVersion != -1 ? valueVersion : updateVersion);
            if (!forceApply) {
                keys.add(key);
                values.add(walValue);
            } else {
                apply.put(new WALKey(prefix, key), walValue);
            }
            return true;
        });

        acquireOne();
        try {
            WALIndex wali = walIndex.get();
            RowsChanged rowsChanged;
            MutableBoolean flushCompactionHint = new MutableBoolean(false);
            MutableLong indexCommittedFromTxId = new MutableLong(Long.MAX_VALUE);
            MutableLong indexCommittedUpToTxId = new MutableLong();

            if (!forceApply) {
                MutableInt i = new MutableInt(0);
                wali.getPointers(
                    (KeyValueStream stream) -> {
                        for (int k = 0; k < keys.size(); k++) {
                            WALValue value = values.get(k);
                            if (!stream
                            .stream(rowType, prefix, keys.get(k), value.getValue(), value.getTimestampId(), value.getTombstoned(), value.getVersion())) {
                                return false;
                            }
                        }
                        return true;
                    },
                    (_rowType, _prefix, key, value, valueTimestamp, valueTombstone, valueVersion, pointerTimestamp, pointerTombstoned, pointerVersion, pointerFp) -> {
                        WALKey walKey = new WALKey(prefix, key);
                        WALValue walValue = new WALValue(rowType, value, valueTimestamp, valueTombstone, valueVersion);
                        if (pointerFp == -1) {
                            apply.put(walKey, walValue);
                        } else if (CompareTimestampVersions.compare(pointerTimestamp, pointerVersion, valueTimestamp, valueVersion) < 0) {
                            apply.put(walKey, walValue);
                            WALTimestampId currentTimestampId = new WALTimestampId(pointerTimestamp, pointerTombstoned);
                            KeyedTimestampId keyedTimestampId = new KeyedTimestampId(walKey.prefix,
                                walKey.key,
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
                rowsChanged = new RowsChanged(versionedPartitionName, apply, removes, clobbers, -1, -1);
            } else {
                List<WALIndexable> indexables = new ArrayList<>(apply.size());
                walTx.write((WALWriter rowWriter) -> {
                    int estimatedSizeInBytes = 0;
                    for (Entry<WALKey, WALValue> row : apply.entrySet()) {
                        byte[] value = row.getValue().getValue();
                        estimatedSizeInBytes += primaryRowMarshaller.maximumSizeInBytes(rowType,
                            row.getKey().sizeOfComposed(),
                            (value != null ? value.length : 0));
                    }
                    flush(rowType,
                        forceTxId,
                        apply.size(),
                        estimatedSizeInBytes,
                        rowStream -> {
                            for (Entry<WALKey, WALValue> row : apply.entrySet()) {
                                WALValue value = row.getValue();
                                if (!rowStream.stream(primaryRowMarshaller.toRow(rowType,
                                    row.getKey().compose(),
                                    value.getValue(),
                                    value.getTimestampId(),
                                    value.getTombstoned(),
                                    value.getVersion()))) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        indexableKeyStream -> {
                            for (Entry<WALKey, WALValue> row : apply.entrySet()) {
                                WALValue value = row.getValue();
                                if (!indexableKeyStream.stream(row.getKey().prefix, row.getKey().key,
                                    value.getTimestampId(), value.getTombstoned(), value.getVersion())) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        indexCommittedFromTxId,
                        indexCommittedUpToTxId,
                        rowWriter,
                        highwater[0],
                        flushCompactionHint,
                        (rowTxId, _prefix, key, valueTimestamp, valueTombstoned, valueVersion, fp) -> {
                            indexables.add(new WALIndexable(rowTxId, prefix, key, valueTimestamp, valueTombstoned, valueVersion, fp));
                            return true;
                        });

                    return null;
                });
                synchronized (oneIndexerAtATimeLock) {
                    wali.merge((TxKeyPointerStream stream) -> {
                        for (WALIndexable ix : indexables) {
                            if (!stream.stream(ix.txId, ix.prefix, ix.key, ix.valueTimestamp, ix.valueTombstoned, ix.valueVersion, ix.fp)) {
                                return false;
                            }

                            mergeStripedKeyHighwaters(ix.prefix, ix.key, ix.valueTimestamp);
                        }
                        return true;
                    }, (mode, txId, _prefix, key, timestamp, tombstoned, version, fp) -> {
                        if (mode == WALMergeKeyPointerStream.added) {
                            keyCount.incrementAndGet();
                        } else if (mode == WALMergeKeyPointerStream.clobbered) {
                            clobberCount.incrementAndGet();
                        } else {
                            apply.remove(new WALKey(prefix, key));
                        }
                        return true;
                    });

                    rowsChanged = new RowsChanged(versionedPartitionName,
                        apply,
                        removes,
                        clobbers,
                        indexCommittedFromTxId.longValue(),
                        indexCommittedUpToTxId.longValue());
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
                        writeIndexCommitMarker(writer, indexCommittedUpToTxId.longValue());
                    }
                    return null;
                });
            }
            return rowsChanged;
        } finally {
            releaseOne();
        }
    }

    private void flush(RowType rowType,
        long txId,
        int estimatedNumberOfRows,
        int estimatedSizeInBytes,
        RawRows rows,
        IndexableKeys indexableKeys,
        MutableLong indexCommittedFromTxId,
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
            if (indexCommittedFromTxId.longValue() > txId) {
                indexCommittedFromTxId.setValue(txId);
            }
            indexCommitedUpToTxId.setValue(txId);
            count = rowWriter.write(txId, rowType, estimatedNumberOfRows, estimatedSizeInBytes, rows, indexableKeys, stream);
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
            return wali.rowScan((prefix, key, timestamp, tombstoned, version, fp) -> {
                byte[] hydrateRowIndexValue = hydrateRowIndexValue(fp);
                RowType rowType = RowType.fromByte(hydrateRowIndexValue[0]);
                byte[] value = primaryRowMarshaller.valueFromRow(rowType, hydrateRowIndexValue, 1 + 8);
                return keyValueStream.stream(rowType, prefix, key, value, timestamp, tombstoned, version);
            });
        } finally {
            releaseOne();
        }
    }

    @Override
    public boolean rangeScan(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey, KeyValueStream keyValueStream) throws Exception {
        acquireOne();
        try {
            WALIndex wali = walIndex.get();
            return wali.rangeScan(fromPrefix,
                fromKey,
                toPrefix,
                toKey,
                (prefix, key, timestamp, tombstoned, version, fp) -> {
                    byte[] hydrateRowIndexValue = hydrateRowIndexValue(fp);
                    RowType rowType = RowType.fromByte(hydrateRowIndexValue[0]);
                    byte[] value = primaryRowMarshaller.valueFromRow(rowType, hydrateRowIndexValue, 1 + 8);
                    return keyValueStream.stream(rowType, prefix, key, value, timestamp, tombstoned, version);
                });
        } finally {
            releaseOne();
        }
    }

    // TODO fix barf
    public TimestampedValue getTimestampedValue(byte[] prefix, byte[] key) throws Exception {
        acquireOne();
        try {
            TimestampedValue[] values = new TimestampedValue[1];
            walIndex.get().getPointer(prefix, key, (_prefix, _key, timestamp, tombstoned, version, fp) -> {
                if (fp != -1 && !tombstoned) {
                    byte[] hydrateRowIndexValue = hydrateRowIndexValue(fp);
                    RowType rowType = RowType.fromByte(hydrateRowIndexValue[0]);
                    byte[] value = primaryRowMarshaller.valueFromRow(rowType, hydrateRowIndexValue, 1 + 8);
                    values[0] = new TimestampedValue(timestamp, version, value);
                }
                return true;
            });
            return values[0];
        } finally {
            releaseOne();
        }
    }

    public boolean streamValues(byte[] prefix, UnprefixedWALKeys keys, KeyValueStream keyValueStream) throws Exception {
        acquireOne();
        try {
            return walIndex.get().getPointers(prefix, keys,
                (_prefix, key, pointerTimestamp, pointerTombstoned, pointerVersion, pointerFp) -> {
                    if (pointerFp != -1) {
                        RowType rowType = null;
                        byte[] value = null;
                        if (!pointerTombstoned) {
                            byte[] hydrateRowIndexValue = hydrateRowIndexValue(pointerFp);
                            rowType = RowType.fromByte(hydrateRowIndexValue[0]);
                            value = primaryRowMarshaller.valueFromRow(rowType, hydrateRowIndexValue, 1 + 8);
                        }

                        return keyValueStream.stream(rowType,
                            prefix,
                            key,
                            value,
                            pointerTimestamp,
                            pointerTombstoned,
                            pointerVersion
                        );

                    } else {
                        return keyValueStream.stream(null, prefix, key, null, -1, false, -1);
                    }
                });
        } finally {
            releaseOne();
        }
    }

    public boolean streamPointers(KeyValues keyValues, KeyValuePointerStream stream) throws Exception {
        acquireOne();
        try {
            return walIndex.get().getPointers(
                indexStream -> keyValues.consume(
                    (rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                        if (valueTimestamp > getLargestTimestampForKeyStripe(prefix, key)) {
                            return stream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion, -1, false, -1, -1);
                        } else {
                            return indexStream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion);
                        }
                    }),
                stream);
        } finally {
            releaseOne();
        }
    }

    public boolean containsKeys(byte[] prefix, UnprefixedWALKeys keys, KeyContainedStream stream) throws Exception {
        acquireOne();
        try {
            return walIndex.get().containsKeys(prefix, keys, stream);
        } finally {
            releaseOne();
        }
    }

    // TODO fix instantiation to hashcode
    private long getLargestTimestampForKeyStripe(byte[] prefix, byte[] key) {
        int highwaterTimestampIndex = Math.abs((Arrays.hashCode(prefix) ^ Arrays.hashCode(key)) % stripedKeyHighwaterTimestamps.length);
        return stripedKeyHighwaterTimestamps[highwaterTimestampIndex];
    }

    //TODO replace with stream!
    private byte[] hydrateRowIndexValue(long indexFP) {
        try {
            return walTx.read((WALReader rowReader) -> rowReader.readTypeByteTxIdAndRow(indexFP));
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
            Boolean readFromTransactionId = walTx.readFromTransactionId(sinceTransactionId, (offset, reader) -> reader.scan(offset, false,
                (rowPointer, rowTxId, rowType, row) -> {
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

    public boolean takeRowUpdatesSince(byte[] prefix, long sinceTransactionId, RowStream rowStream) throws Exception {
        if (sinceTransactionId >= highestTxId.get()) {
            return true;
        }
        acquireOne();
        try {
            long[] takeMetrics = new long[1];
            WALIndex wali = walIndex.get();
            Boolean readFromTransactionId = walTx.read(reader
                -> reader.read(fpStream -> wali.takePrefixUpdatesSince(prefix, sinceTransactionId, (txId, fp) -> fpStream.stream(fp)),
                    (rowPointer, rowTxId, rowType, row) -> {
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
            return walTx.readFromTransactionId(0, (offset, reader) -> reader.scan(offset, false, rowStream::row));
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

    public long count(WALKeyPointers keyPointers) throws Exception {
        acquireOne();
        try {
            return walIndex.get().deltaCount(keyPointers) + keyCount.get();
        } finally {
            releaseOne();
        }
    }

    public long highestTxId() {
        return highestTxId.get();
    }
}
