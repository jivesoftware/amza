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
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.RangeScannable;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.scan.TxKeyValueStream;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.KeyContainedStream;
import com.jivesoftware.os.amza.shared.wal.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.KeyValues;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

public class WALStorage implements RangeScannable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int numTickleMeElmaphore = 1024; // TODO config

    private final VersionedPartitionName versionedPartitionName;
    private final OrderIdProvider orderIdProvider;
    private final PrimaryRowMarshaller<byte[]> rowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    private final WALTx walTx;
    private final AtomicInteger maxUpdatesBetweenCompactionHintMarker;
    private final AtomicInteger maxUpdatesBetweenIndexCommitMarker;
    private final MutableLong[] stripedKeyHighwaterTimestamps;

    private final AtomicReference<WALIndex> walIndex = new AtomicReference<>(null);
    private final Object oneIndexerAtATimeLock = new Object();
    private final Object oneTransactionAtATimeLock = new Object();
    private final Semaphore tickleMeElmophore = new Semaphore(numTickleMeElmaphore, true);
    private final AtomicLong updateCount = new AtomicLong(0);
    private final AtomicLong newCount = new AtomicLong(0);
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
        WALTx walTx,
        int maxUpdatesBetweenCompactionHintMarker,
        int maxUpdatesBetweenIndexCommitMarker,
        int tombstoneCompactionFactor) {

        this.versionedPartitionName = versionedPartitionName;
        this.orderIdProvider = orderIdProvider;
        this.rowMarshaller = rowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.walTx = walTx;
        this.maxUpdatesBetweenCompactionHintMarker = new AtomicInteger(maxUpdatesBetweenCompactionHintMarker);
        this.maxUpdatesBetweenIndexCommitMarker = new AtomicInteger(maxUpdatesBetweenIndexCommitMarker);
        this.tombstoneCompactionFactor = tombstoneCompactionFactor;

        int numKeyHighwaterStripes = 1024; // TODO expose to config
        this.stripedKeyHighwaterTimestamps = new MutableLong[numKeyHighwaterStripes];
        for (int i = 0; i < stripedKeyHighwaterTimestamps.length; i++) {
            stripedKeyHighwaterTimestamps[i] = new MutableLong();
        }
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
            WALIndex wali = walIndex.get();
            if (wali != null) {
                expunged &= wali.delete();
            }
        } finally {
            releaseAll();
        }
        return expunged;
    }

    public boolean compactableTombstone(long removeTombstonedOlderTimestampId, long ttlTimestampId) throws Exception {
        return (clobberCount.get() + 1) / (newCount.get() + 1) > tombstoneCompactionFactor;
    }

    public long compactTombstone(long removeTombstonedOlderThanTimestampId, long ttlTimestampId) throws Exception {
        final String metricPrefix = "partition>" + versionedPartitionName.getPartitionName().getName()
            + ">ring>" + versionedPartitionName.getPartitionName().getRingName() + ">";
        Optional<WALTx.Compacted> compact = walTx.compact(removeTombstonedOlderThanTimestampId, ttlTimestampId, walIndex.get());
        if (compact.isPresent()) {
            acquireAll();
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

            final MutableLong lastTxId = new MutableLong(0);
            final AtomicLong rowsVisited = new AtomicLong(maxUpdatesBetweenCompactionHintMarker.get());
            final AtomicBoolean gotCompactionHints = new AtomicBoolean(false);
            final AtomicBoolean gotHighestTxId = new AtomicBoolean(false);

            walTx.read((WALReader rowReader) -> {
                rowReader.reverseScan((rowPointer, rowTxId, rowType, row) -> {
                    if (rowTxId > lastTxId.longValue()) {
                        lastTxId.setValue(rowTxId);
                        gotHighestTxId.set(true);
                    }
                    if (!gotCompactionHints.get() && rowType == RowType.system) {
                        ByteBuffer buf = ByteBuffer.wrap(row);
                        byte[] keyBytes = new byte[8];
                        buf.get(keyBytes);
                        long key = UIO.bytesLong(keyBytes);
                        if (key == RowType.COMPACTION_HINTS_KEY) {
                            byte[] newCountBytes = new byte[8];
                            byte[] clobberCountBytes = new byte[8];
                            buf.get(newCountBytes);
                            buf.get(clobberCountBytes);
                            newCount.set(UIO.bytesLong(newCountBytes));
                            clobberCount.set(UIO.bytesLong(clobberCountBytes));
                            gotCompactionHints.set(true);
                        }
                    }
                    return !gotHighestTxId.get() || (rowsVisited.decrementAndGet() >= 0 && !gotCompactionHints.get());
                });
                return null;
            });
            highestTxId.set(lastTxId.longValue());

            walTx.write((WALWriter writer) -> {
                writeCompactionHintMarker(writer);
                return null;
            });

        } finally {
            releaseAll();
        }
    }

    public void flush(boolean fsync) throws Exception {
        walTx.flush(fsync);
    }

    public boolean delete(boolean ifEmpty) throws Exception {
        throw new UnsupportedOperationException("Not yet!");
    }

    private void writeCompactionHintMarker(WALWriter rowWriter) throws Exception {
        synchronized (oneTransactionAtATimeLock) {
            rowWriter.writeSystem(UIO.longsBytes(new long[] {
                RowType.COMPACTION_HINTS_KEY,
                newCount.get(),
                clobberCount.get()
            }));
            updateCount.set(0);
        }
    }

    private void writeIndexCommitMarker(WALWriter rowWriter, long indexCommitedUpToTxId) throws Exception {
        synchronized (oneTransactionAtATimeLock) {
            rowWriter.writeSystem(UIO.longsBytes(new long[] {
                RowType.COMMIT_KEY,
                indexCommitedUpToTxId }));
        }
    }

    public void writeHighwaterMarker(WALWriter rowWriter, WALHighwater highwater) throws Exception {
        synchronized (oneTransactionAtATimeLock) {
            rowWriter.writeHighwater(highwaterRowMarshaller.toBytes(highwater));
        }
    }

    /*TODO if we care, persist stripedKeyHighwaterTimestamps periodically as a cold start optimization for getLargestTimestampForKeyStripe()
     private void writeStripedTimestamp(WALWriter rowWriter) throws Exception {
     synchronized (oneTransactionAtATimeLock) {
     rowWriter.write(
     Collections.singletonList(-1L),
     Collections.singletonList(WALWriter.SYSTEM_VERSION_1),
     Collections.singletonList(UIO.longsBytes(new long[]{
     WALWriter.STRIPE_TIME_MARKER,
     stripedKeyHighwaterTimestamps})));
     }
     }
     */
    public RowsChanged update(boolean useUpdateTxId,
        Commitable updates) throws Exception {

        final AtomicLong oldestAppliedTimestamp = new AtomicLong(Long.MAX_VALUE);
        final Table<Long, WALKey, WALValue> apply = TreeBasedTable.create();
        final Map<WALKey, WALTimestampId> removes = new HashMap<>();
        final Map<WALKey, WALTimestampId> clobbers = new HashMap<>();

        final List<Long> transactionIds = useUpdateTxId ? new ArrayList<>() : null;
        final List<byte[]> keys = new ArrayList<>();
        final List<WALValue> values = new ArrayList<>();
        final Map<Long, WALHighwater> txIdToHighwater = new HashMap<>();

        updates.commitable((highwater) -> {
            if (useUpdateTxId) {
                txIdToHighwater.put(transactionIds.get(transactionIds.size() - 1), highwater);
            } else {
                txIdToHighwater.put(-1L, highwater);
            }
        }, (transactionId, key, value, valueTimestamp, valueTombstoned) -> {
            if (useUpdateTxId) {
                transactionIds.add(transactionId);
            }
            keys.add(key);
            values.add(new WALValue(value, valueTimestamp, valueTombstoned));
            return true;
        });

        acquireOne();
        try {
            WALIndex wali = walIndex.get();
            RowsChanged rowsChanged;
            MutableBoolean flushCompactionHint = new MutableBoolean(false);
            MutableLong indexCommitedUpToTxId = new MutableLong();

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
                long txId = useUpdateTxId ? transactionIds.get(i.intValue()) : -1L;
                WALValue walValue = new WALValue(value, valueTimestamp, valueTombstone);
                if (pointerFp == -1) {
                    apply.put(txId, walKey, walValue);
                    if (oldestAppliedTimestamp.get() > valueTimestamp) {
                        oldestAppliedTimestamp.set(valueTimestamp);
                    }
                } else if (pointerTimestamp < valueTimestamp) {
                    apply.put(txId, walKey, walValue);
                    if (oldestAppliedTimestamp.get() > valueTimestamp) {
                        oldestAppliedTimestamp.set(valueTimestamp);
                    }
                    //TODO do we REALLY need the old value? WALValue value = hydrateRowIndexValue(currentPointer);
                    WALTimestampId currentTimestampId = new WALTimestampId(pointerTimestamp, pointerTombstoned);
                    clobbers.put(walKey, currentTimestampId);
                    if (valueTombstone && !pointerTombstoned) {
                        removes.put(walKey, currentTimestampId);
                    }
                }
                i.increment();
                return true;
            });

            if (apply.isEmpty()) {
                rowsChanged = new RowsChanged(versionedPartitionName, oldestAppliedTimestamp.get(), apply, removes, clobbers, -1);
            } else {
                List<WALIndexable> indexables = new ArrayList<>(apply.size());
                walTx.write((WALWriter rowWriter) -> {
                    for (Long txId : apply.rowKeySet()) {
                        Map<WALKey, WALValue> txRow = apply.row(txId);
                        flush(useUpdateTxId ? txId : -1,
                            rowStream -> {
                                for (Entry<WALKey, WALValue> row : txRow.entrySet()) {
                                    WALValue value = row.getValue();
                                    if (!rowStream.stream(rowMarshaller.toRow(row.getKey().getKey(),
                                        value.getValue(),
                                        value.getTimestampId(),
                                        value.getTombstoned()))) {
                                        return false;
                                    }
                                }
                                return true;
                            },
                            indexableKeyStream -> {
                                for (Entry<WALKey, WALValue> row : apply.row(txId).entrySet()) {
                                    WALValue value = row.getValue();
                                    if (!indexableKeyStream.stream(row.getKey().getKey(), value.getTimestampId(), value.getTombstoned())) {
                                        return false;
                                    }
                                }
                                return true;
                            },
                            indexCommitedUpToTxId,
                            rowWriter,
                            txIdToHighwater.get(txId),
                            flushCompactionHint,
                            (rowTxId, key, valueTimestamp, valueTombstoned, fp) -> {
                                indexables.add(new WALIndexable(rowTxId, key, valueTimestamp, valueTombstoned, fp));
                                return true;
                            });
                    }

                    return null;
                });
                synchronized (oneIndexerAtATimeLock) {
                    wali.merge((TxKeyPointerStream stream) -> {
                        for (WALIndexable indexable : indexables) {
                            if (!stream.stream(indexable.txId, indexable.key, indexable.valueTimestamp, indexable.valueTombstoned, indexable.fp)) {
                                return false;
                            }

                            int highwaterTimestampIndex = Math.abs(Arrays.hashCode(indexable.key) % stripedKeyHighwaterTimestamps.length);
                            stripedKeyHighwaterTimestamps[highwaterTimestampIndex].setValue(Math.max(
                                stripedKeyHighwaterTimestamps[highwaterTimestampIndex].longValue(),
                                indexable.valueTimestamp));
                        }
                        return true;
                    }, (mode, txId, key, timestamp, tombstoned, fp) -> {
                        if (mode == WALMergeKeyPointerStream.added) {
                            newCount.incrementAndGet();
                        } else if (mode == WALMergeKeyPointerStream.clobbered) {
                            clobberCount.incrementAndGet();
                        } else {
                            apply.remove(useUpdateTxId ? txId : -1L, new WALKey(key)); //TODO NOT THIS
                        }
                        return true;
                    });

                    rowsChanged = new RowsChanged(versionedPartitionName, oldestAppliedTimestamp.get(), apply, removes, clobbers,
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
        return stripedKeyHighwaterTimestamps[highwaterTimestampIndex].longValue();
    }

    private byte[] hydrateRowIndexValue(long indexFP) {
        try {
            byte[] row = walTx.read((WALReader rowReader) -> rowReader.read(indexFP));
            return rowMarshaller.valueFromRow(row);
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
        acquireOne();
        try {
            return walTx.readFromTransactionId(sinceTransactionId, (long offset, WALReader rowReader) -> rowReader.scan(offset, false,
                (long rowPointer, long rowTxId, RowType rowType, byte[] row) -> {
                    if (rowType != RowType.system && rowTxId > sinceTransactionId) {
                        return rowStream.row(rowPointer, rowTxId, rowType, row);
                    }
                    return true;
                }));
        } finally {
            releaseOne();
        }
    }

    /**
     * @param sinceTransactionId
     * @param highwaters         Nullable
     * @param txKeyValueStream
     * @return
     * @throws Exception
     */
    public boolean takeFromTransactionId(long sinceTransactionId, Highwaters highwaters, TxKeyValueStream txKeyValueStream) throws Exception {
        acquireOne();
        try {
            return walTx.readFromTransactionId(sinceTransactionId, (long offset, WALReader rowReader) -> rowReader.scan(offset, false,
                (rowPointer, rowTxId, rowType, row) -> {
                    if (rowType == RowType.highwater && highwaters != null) {
                        WALHighwater highwater = highwaterRowMarshaller.fromBytes(row);
                        highwaters.highwater(highwater);
                    } else if (rowType == RowType.primary && rowTxId > sinceTransactionId) {
                        return rowMarshaller.fromRow(row, rowTxId, txKeyValueStream);
                    }
                    return true;
                }));
        } finally {
            releaseOne();
        }
    }

    public boolean takeRowsFromTransactionId(long sinceTransactionId, RowStream rowStream) throws Exception {
        acquireOne();
        try {
            return walTx.readFromTransactionId(sinceTransactionId, (offset, reader) -> {
                return reader.scan(offset, false, rowStream);
            });
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
        acquireOne();
        try {
            WALIndex wali = this.walIndex.get();
            return wali.size();
        } finally {
            releaseOne();
        }
    }

    public long highestTxId() {
        return highestTxId.get();
    }
}
