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

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.api.CompareTimestampVersions;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.RangeScannable;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.KeyContainedStream;
import com.jivesoftware.os.amza.api.stream.KeyValuePointerStream;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.KeyValues;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyPointerStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.stream.WALKeyPointers;
import com.jivesoftware.os.amza.api.stream.WALMergeKeyPointerStream;
import com.jivesoftware.os.amza.api.wal.KeyedTimestampId;
import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALIndex;
import com.jivesoftware.os.amza.api.wal.WALIndexProvider;
import com.jivesoftware.os.amza.api.wal.WALIndexable;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALTimestampId;
import com.jivesoftware.os.amza.api.wal.WALTx;
import com.jivesoftware.os.amza.api.wal.WALValue;
import com.jivesoftware.os.amza.api.wal.WALWriter;
import com.jivesoftware.os.amza.api.wal.WALWriter.IndexableKeys;
import com.jivesoftware.os.amza.api.wal.WALWriter.RawRows;
import com.jivesoftware.os.amza.api.wal.WALWriter.TxKeyPointerFpStream;
import com.jivesoftware.os.amza.service.SickPartitions;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

public class WALStorage<I extends WALIndex> implements RangeScannable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int numTickleMeElmaphore = 1024; // TODO config
    private static final int numKeyHighwaterStripes = 1024; // TODO expose to config

    private final AmzaStats amzaStats;
    private final VersionedPartitionName versionedPartitionName;
    private final OrderIdProvider orderIdProvider;
    private final PrimaryRowMarshaller primaryRowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    private final WALTx walTx;
    private final WALIndexProvider<I> walIndexProvider;
    private final SickPartitions sickPartitions;
    private final boolean hardFsyncBeforeLeapBoundary;
    private volatile long[] stripedKeyHighwaterTimestamps;

    private final AtomicReference<I> walIndex = new AtomicReference<>(null);
    private final Object oneIndexerAtATimeLock = new Object();
    private final Object oneTransactionAtATimeLock = new Object();
    private final Semaphore tickleMeElmophore = new Semaphore(numTickleMeElmaphore, true);
    private final AtomicLong oldestTimestamp = new AtomicLong(-1);
    private final AtomicLong oldestVersion = new AtomicLong(-1);
    private final AtomicLong oldestTombstonedTimestamp = new AtomicLong(-1);
    private final AtomicLong oldestTombstonedVersion = new AtomicLong(-1);
    private final AtomicLong keyCount = new AtomicLong(0);
    private final AtomicLong clobberCount = new AtomicLong(0);
    private final AtomicLong highestTxId = new AtomicLong(-1);
    private final AtomicBoolean hasEndOfMergeMarker = new AtomicBoolean(false);
    private final int tombstoneCompactionFactor;

    private final ThreadLocal<Integer> reentrant = new ReentrantThreadLocal();
    private final AtomicBoolean sick = new AtomicBoolean();

    private volatile long mergedTxId = -1;

    static class ReentrantThreadLocal extends ThreadLocal<Integer> {

        @Override
        protected Integer initialValue() {
            return 0;
        }
    }

    public WALStorage(AmzaStats amzaStats,
        VersionedPartitionName versionedPartitionName,
        OrderIdProvider orderIdProvider,
        PrimaryRowMarshaller rowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller,
        WALTx walTx,
        WALIndexProvider<I> walIndexProvider,
        SickPartitions sickPartitions,
        boolean hardFsyncBeforeLeapBoundary,
        int tombstoneCompactionFactor) {
        this.amzaStats = amzaStats;

        this.versionedPartitionName = versionedPartitionName;
        this.orderIdProvider = orderIdProvider;
        this.primaryRowMarshaller = rowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.walTx = walTx;
        this.walIndexProvider = walIndexProvider;
        this.sickPartitions = sickPartitions;
        this.hardFsyncBeforeLeapBoundary = hardFsyncBeforeLeapBoundary;
        this.tombstoneCompactionFactor = tombstoneCompactionFactor;
        this.stripedKeyHighwaterTimestamps = null;
    }

    public boolean isSick() {
        return sick.get();
    }

    private void acquireOne() {
        if (sick.get()) {
            throw new IllegalStateException("Partition is sick: " + versionedPartitionName);
        }
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
            reentrant.remove();
        } else {
            reentrant.set(enters - 1);
        }
    }

    private void acquireAll() throws InterruptedException {
        if (sick.get()) {
            throw new IllegalStateException("Partition is sick: " + versionedPartitionName);
        }
        tickleMeElmophore.acquire(numTickleMeElmaphore);
    }

    private void releaseAll() {
        tickleMeElmophore.release(numTickleMeElmaphore);
    }

    public VersionedPartitionName getVersionedPartitionName() {
        return versionedPartitionName;
    }

    public void delete(File baseKey) throws Exception {
        acquireAll();
        try {
            walTx.delete(baseKey);
            I wali = walIndex.get();
            if (wali != null) {
                wali.delete();
                walIndex.set(null);
                sickPartitions.recovered(versionedPartitionName);
            }
        } finally {
            releaseAll();
        }
    }

    public boolean compactableTombstone(long tombstoneTimestampId,
        long tombstoneVersion,
        long ttlTimestampId,
        long ttlVersion) throws Exception {

        long compactableOldestTombstonedTimestamp = oldestTombstonedTimestamp.get();
        long compactableOldestTombstonedVersion = oldestTombstonedVersion.get();
        long compactableOldestTimestamp = oldestTimestamp.get();
        long compactableOldestVersion = oldestVersion.get();
        return (compactableOldestTombstonedTimestamp > -1 && compactableOldestTombstonedTimestamp < tombstoneTimestampId)
            || (compactableOldestTombstonedVersion > -1 && compactableOldestTombstonedVersion < tombstoneVersion)
            || (compactableOldestTimestamp > -1 && compactableOldestTimestamp < ttlTimestampId)
            || (compactableOldestVersion > -1 && compactableOldestVersion < ttlVersion)
            || ((clobberCount.get() + 1) / (keyCount.get() + 1) > tombstoneCompactionFactor);
    }

    public interface TxTransitionToCompacted {

        public interface TransitionToCompactedTx {

            long tx(Callable<Void> completedCompactCommit) throws Exception;
        }

        long tx(TransitionToCompactedTx transitionToCompacted) throws Exception;
    }

    public long compactTombstone(File fromBaseKey,
        File toBaseKey,
        RowType rowType,
        long tombstoneTimestampId,
        long tombstoneVersion,
        long ttlTimestampId,
        long ttlVersion,
        int stripe,
        boolean expectedEndOfMerge,
        TxTransitionToCompacted transitionToCompacted) throws Exception {

        if (expectedEndOfMerge && !hasEndOfMergeMarker.get()) {
            return 0;
        }
        I got = walIndex.get();
        WALTx.Compacted<I> compact = walTx.compact(fromBaseKey,
            toBaseKey,
            rowType,
            tombstoneTimestampId,
            tombstoneVersion,
            ttlTimestampId,
            ttlVersion,
            got,
            stripe);

        long[] compactKeyHighwaterTimestamps = findKeyHighwaterTimestamps();

        return transitionToCompacted.tx((completedCompactCommit) -> {

            acquireAll();
            try {
                WALTx.CommittedCompacted<I> compacted;
                try {
                    compacted = compact.commit((!expectedEndOfMerge) ? null
                        : (raw,
                            highestTxId,
                            oldestTimestamp,
                            oldestVersion,
                            oldestTombstonedTimestamp,
                            oldestTombstonedVersion,
                            keyCount,
                            fpOfLastLeap,
                            updatesSinceLeap) -> {

                            long[] oldMarker = loadEndOfMergeMarker(-1, raw);
                            if (oldMarker == null) {
                                throw new IllegalStateException("Invalid end of merge marker");
                            }
                            long[] marker = buildEndOfMergeMarker(oldMarker[EOM_DELTA_WAL_ID_INDEX],
                                highestTxId,
                                oldestTimestamp,
                                oldestVersion,
                                oldestTombstonedTimestamp,
                                oldestTombstonedVersion,
                                keyCount,
                                0,
                                fpOfLastLeap,
                                updatesSinceLeap,
                                compactKeyHighwaterTimestamps,
                                0);

                            return UIO.longsBytes(marker);
                        }, completedCompactCommit);
                } catch (Exception e) {
                    LOG.inc("failedCompaction");
                    LOG.error("Failed to compact {}, attempting to reload", new Object[]{versionedPartitionName}, e);
                    loadInternal(fromBaseKey, -1, -1, true, false, false, stripe);
                    return -1L;
                }
                walIndex.set(compacted.index);
                keyCount.set(compacted.keyCount);
                clobberCount.set(0);
                oldestTimestamp.set(compacted.oldestTimestamp);
                oldestVersion.set(compacted.oldestVersion);
                oldestTombstonedTimestamp.set(compacted.oldestTombstonedTimestamp);
                oldestTombstonedVersion.set(compacted.oldestTombstonedVersion);

                LOG.info("Completed compaction: {}", compacted);
                return compacted.sizeAfterCompaction;
            } finally {
                releaseAll();
            }
        });
    }

    public void load(File baseKey, long deltaWALId, long prevDeltaWALId, boolean backwardScan, boolean truncateToEndOfMergeMarker, int stripe) throws Exception {
        acquireAll();
        try {
            loadInternal(baseKey, deltaWALId, prevDeltaWALId, false, backwardScan, truncateToEndOfMergeMarker, stripe);
        } finally {
            releaseAll();
        }
    }

    private void loadInternal(File baseKey,
        long deltaWALId,
        long prevDeltaWALId,
        boolean recovery,
        boolean backwardScan,
        boolean truncateToEndOfMergeMarker,
        int stripe) throws Exception {
        try {

            long initialHighestTxId = highestTxId.get();
            if (!recovery && initialHighestTxId != -1) {
                throw new IllegalStateException("Load should have completed before highestTxId:" + initialHighestTxId + " is modified.");
            }

            walTx.open(baseKey, io -> {
                boolean[] endOfMergeMarker = {false};
                long[] lastTxId = {-1};
                long[] fpOfLastLeap = {-1};
                long[] updatesSinceLastMergeMarker = {0};
                long[] updatesSinceLastLeap = {0};

                long[] trailingDeltaWALId = {-1};
                long[] loadOldestTimestamp = {-1};
                long[] loadOldestVersion = {-1};
                long[] loadOldestTombstonedTimestamp = {-1};
                long[] loadOldestTombstonedVersion = {-1};
                long[] loadKeyCount = {0};
                long[] loadClobberCount = {0};
                long[] loadKeyHighwaterTimestamps = versionedPartitionName.getPartitionName().isSystemPartition() ? new long[numKeyHighwaterStripes] : null;

                long[] truncate = {0};
                primaryRowMarshaller.fromRows(fpRowStream -> {
                    io.validate(backwardScan,
                        truncateToEndOfMergeMarker,
                        (rowFP, rowTxId, rowType, row) -> {
                            // backward scan
                            if (rowType == RowType.end_of_merge) {
                                long[] marker = loadEndOfMergeMarker(deltaWALId, row);
                                if (marker == null) {
                                    return -1;
                                }

                                endOfMergeMarker[0] = true;
                                trailingDeltaWALId[0] = marker[EOM_DELTA_WAL_ID_INDEX];
                                lastTxId[0] = Math.max(lastTxId[0], marker[EOM_HIGHEST_TX_ID_INDEX]);
                                loadOldestTimestamp[0] = marker[EOM_OLDEST_TIMESTAMP_INDEX];
                                loadOldestVersion[0] = marker[EOM_OLDEST_VERSION_INDEX];
                                loadOldestTombstonedTimestamp[0] = marker[EOM_OLDEST_TOMBSTONED_TIMESTAMP_INDEX];
                                loadOldestTombstonedVersion[0] = marker[EOM_OLDEST_TOMBSTONED_VERSION_INDEX];
                                loadKeyCount[0] = marker[EOM_KEY_COUNT_INDEX];
                                loadClobberCount[0] = marker[EOM_CLOBBER_COUNT_INDEX];
                                //markerStripedTimestamps[0] = marker;

                                fpOfLastLeap[0] = marker[EOM_FP_OF_LAST_LEAP_INDEX];
                                updatesSinceLastLeap[0] = marker[EOM_UPDATES_SINCE_LAST_LEAP_INDEX];
                                return rowFP;
                            }
                            return -1;
                        },
                        (rowFP, rowTxId, rowType, row) -> {
                            // forward scan, only called if backward scan was unsuccessful
                            if (rowType.isPrimary()) {
                                lastTxId[0] = Math.max(lastTxId[0], rowTxId);
                                updatesSinceLastMergeMarker[0]++;
                                updatesSinceLastLeap[0]++;
                                try {
                                    Preconditions.checkState(fpRowStream.stream(rowFP, rowType, row), "Validation must accept all primary rows");
                                } catch (IOException e) {
                                    LOG.error("Encountered I/O exception during forward validation for {}, WAL must be truncated",
                                        new Object[]{versionedPartitionName}, e);
                                    return rowFP;
                                }
                            } else if (rowType == RowType.end_of_merge) {
                                long[] marker = loadEndOfMergeMarker(deltaWALId, row);
                                if (marker == null) {
                                    LOG.error("Encountered corrupt end of merge marker during forward validation for {}, WAL must be truncated",
                                        versionedPartitionName);
                                    return rowFP;
                                }

                                updatesSinceLastMergeMarker[0] = 0;

                                endOfMergeMarker[0] = true;
                                trailingDeltaWALId[0] = marker[EOM_DELTA_WAL_ID_INDEX];
                                lastTxId[0] = Math.max(lastTxId[0], marker[EOM_HIGHEST_TX_ID_INDEX]);
                                loadOldestTimestamp[0] = Math.min(loadOldestTimestamp[0], marker[EOM_OLDEST_TIMESTAMP_INDEX]);
                                loadOldestVersion[0] = Math.min(loadOldestVersion[0], marker[EOM_OLDEST_VERSION_INDEX]);
                                loadOldestTombstonedTimestamp[0] = Math.min(loadOldestTombstonedTimestamp[0], marker[EOM_OLDEST_TOMBSTONED_TIMESTAMP_INDEX]);
                                loadOldestTombstonedVersion[0] = Math.min(loadOldestTombstonedVersion[0], marker[EOM_OLDEST_TOMBSTONED_VERSION_INDEX]);
                                loadKeyCount[0] = marker[EOM_KEY_COUNT_INDEX];
                                loadClobberCount[0] = marker[EOM_CLOBBER_COUNT_INDEX];
                                //markerStripedTimestamps[0] = marker;

                                if (truncateToEndOfMergeMarker) {
                                    truncate[0] = rowFP;
                                }
                            } else if (rowType == RowType.system) {
                                ByteBuffer buf = ByteBuffer.wrap(row);
                                byte[] keyBytes = new byte[8];
                                buf.get(keyBytes);
                                long key = UIO.bytesLong(keyBytes);
                                if (key == RowType.LEAP_KEY) {
                                    fpOfLastLeap[0] = rowFP;
                                    updatesSinceLastLeap[0] = 0;
                                }
                            }
                            if (!truncateToEndOfMergeMarker) {
                                truncate[0] = rowFP;
                            }
                            return -(truncate[0] + 1);
                        },
                        null);

                    // TODO!!!! handle the discontinguity of endOfMerge delta ids which prevents us from doing trailingDeltaWALId[0] != prevDeltaWALId
                    if (truncateToEndOfMergeMarker) {
                        if (prevDeltaWALId > -1 && trailingDeltaWALId[0] > prevDeltaWALId) {
                            LOG.error("Inconsistency detected while loading delta:{} prev:{}, encountered tail:{} for {}",
                                deltaWALId, prevDeltaWALId, trailingDeltaWALId[0], versionedPartitionName);

                            // Since we added the ability to rebalance this IllegalStateException needs to be reworked so that it works.
//                            throw new IllegalStateException("Delta mismatch, intervention is required. "
//                                + "delta:" + deltaWALId
//                                + " prev:" + prevDeltaWALId
//                                + ", encountered tail:" + trailingDeltaWALId[0]
//                                + " for " + versionedPartitionName);
                        }
                    }
                    return true;
                }, (fp, rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                    loadOldestTimestamp[0] = Math.min(loadOldestTimestamp[0], valueTimestamp);
                    loadOldestVersion[0] = Math.min(loadOldestVersion[0], valueVersion);
                    if (valueTombstoned) {
                        loadOldestTombstonedTimestamp[0] = Math.min(loadOldestTombstonedTimestamp[0], valueTimestamp);
                        loadOldestTombstonedVersion[0] = Math.min(loadOldestTombstonedVersion[0], valueVersion);
                    }
                    if (loadKeyHighwaterTimestamps != null) {
                        mergeStripedKeyHighwaters(prefix, key, valueTimestamp, loadKeyHighwaterTimestamps);
                    }
                    return true;
                });

                hasEndOfMergeMarker.set(endOfMergeMarker[0]);
                highestTxId.set(lastTxId[0]);
                mergedTxId = lastTxId[0];
                oldestTimestamp.set(loadOldestTimestamp[0]);
                oldestVersion.set(loadOldestVersion[0]);
                oldestTombstonedTimestamp.set(loadOldestTombstonedTimestamp[0]);
                oldestTombstonedVersion.set(loadOldestTombstonedVersion[0]);
                keyCount.set(loadKeyCount[0] + updatesSinceLastMergeMarker[0]);
                clobberCount.set(loadClobberCount[0]);

                io.initLeaps(fpOfLastLeap[0], updatesSinceLastLeap[0]);

                if (loadKeyHighwaterTimestamps != null) {
                    LOG.info("Loaded highwater timestamps for {}", versionedPartitionName);
                    stripedKeyHighwaterTimestamps = loadKeyHighwaterTimestamps;
                }
                /*if (markerStripedTimestamps[0] != null) {
                    for (int i = 0, offset = EOM_HIGHWATER_STRIPES_OFFSET; i < numKeyHighwaterStripes; i++, offset++) {
                        stripedKeyHighwaterTimestamps[i] = Math.max(stripedKeyHighwaterTimestamps[i], markerStripedTimestamps[0][offset]);
                    }
                }*/

                return null;
            });

            I index = walTx.openIndex(baseKey, walIndexProvider, versionedPartitionName, stripe);
            walIndex.compareAndSet(null, index);

        } catch (Exception e) {
            LOG.error("Partition {} could not be opened, intervention is required, partition will be parked, recovery:{}",
                new Object[]{versionedPartitionName, recovery}, e);
            sick.set(true);
            sickPartitions.sick(versionedPartitionName, e);
        }
    }

    public void flush(boolean fsync) throws Exception {
        walTx.flush(fsync);
    }

    public WALIndex commitIndex(boolean fsync, long txId) throws Exception {
        WALIndex wali = walIndex.get();
        if (wali != null) {
            clearKeyHighwaterTimestamps();
            wali.commit(fsync);
        } else {
            LOG.warn("Trying to commit a nonexistent index:{}.", versionedPartitionName);
        }
        mergedTxId = txId;
        return wali;
    }

    public void endOfMergeMarker(long deltaWALId, long highestTxId) throws Exception {
        long[] keyHighwaterTimestamps = findKeyHighwaterTimestamps();
        walTx.tx((io) -> {
            long[] marker = buildEndOfMergeMarker(deltaWALId,
                highestTxId,
                oldestTimestamp.get(),
                oldestVersion.get(),
                oldestTombstonedTimestamp.get(),
                oldestTombstonedVersion.get(),
                keyCount.get(),
                clobberCount.get(),
                io.getFpOfLastLeap(),
                io.getUpdatesSinceLeap(),
                keyHighwaterTimestamps,
                0);
            byte[] endOfMergeMarker = UIO.longsBytes(marker);

            io.write(-1,
                RowType.end_of_merge,
                1,
                endOfMergeMarker.length,
                stream -> stream.stream(endOfMergeMarker),
                stream -> true,
                (txId, prefix, key, valueTimestamp, valueTombstoned, valueVersion, fp) -> true,
                false,
                hardFsyncBeforeLeapBoundary);
            return null;
        });
        hasEndOfMergeMarker.set(true);
    }

    private static final int EOM_VERSION_INDEX = 0;
    private static final int EOM_CHECKSUM_INDEX = 1;
    private static final int EOM_DELTA_WAL_ID_INDEX = 2;
    private static final int EOM_HIGHEST_TX_ID_INDEX = 3;
    private static final int EOM_OLDEST_TIMESTAMP_INDEX = 4;
    private static final int EOM_OLDEST_VERSION_INDEX = 5;
    private static final int EOM_OLDEST_TOMBSTONED_TIMESTAMP_INDEX = 6;
    private static final int EOM_OLDEST_TOMBSTONED_VERSION_INDEX = 7;
    private static final int EOM_KEY_COUNT_INDEX = 8;
    private static final int EOM_CLOBBER_COUNT_INDEX = 9;
    private static final int EOM_FP_OF_LAST_LEAP_INDEX = 10;
    private static final int EOM_UPDATES_SINCE_LAST_LEAP_INDEX = 11;
    private static final int EOM_HIGHWATER_STRIPES_OFFSET = 12;

    private static long[] buildEndOfMergeMarker(long deltaWALId,
        long highestTxId,
        long oldestTimestamp,
        long oldestVersion,
        long oldestTombstonedTimestamp,
        long oldestTombstonedVersion,
        long keyCount,
        long clobberCount,
        long fpOfLastLeap,
        long updatesSinceLeap,
        long[] stripedKeyHighwaterTimestamps,
        int offset) {
        final long[] marker = new long[EOM_HIGHWATER_STRIPES_OFFSET + numKeyHighwaterStripes];
        marker[EOM_VERSION_INDEX] = 1; // version
        marker[EOM_CHECKSUM_INDEX] = 0; // placeholder checksum
        marker[EOM_DELTA_WAL_ID_INDEX] = deltaWALId;
        marker[EOM_HIGHEST_TX_ID_INDEX] = highestTxId;
        marker[EOM_OLDEST_TIMESTAMP_INDEX] = oldestTimestamp;
        marker[EOM_OLDEST_VERSION_INDEX] = oldestVersion;
        marker[EOM_OLDEST_TOMBSTONED_TIMESTAMP_INDEX] = oldestTombstonedTimestamp;
        marker[EOM_OLDEST_TOMBSTONED_VERSION_INDEX] = oldestTombstonedVersion;
        marker[EOM_KEY_COUNT_INDEX] = keyCount;
        marker[EOM_CLOBBER_COUNT_INDEX] = clobberCount;

        marker[EOM_FP_OF_LAST_LEAP_INDEX] = fpOfLastLeap;
        marker[EOM_UPDATES_SINCE_LAST_LEAP_INDEX] = updatesSinceLeap;

        System.arraycopy(stripedKeyHighwaterTimestamps, offset, marker, EOM_HIGHWATER_STRIPES_OFFSET, numKeyHighwaterStripes);

        CRC32 crC32 = new CRC32();
        byte[] hintsAsBytes = UIO.longsBytes(marker);
        crC32.update(hintsAsBytes, 16, hintsAsBytes.length - 16); // 16 skips the version and checksum
        marker[EOM_CHECKSUM_INDEX] = crC32.getValue();
        return marker;
    }

    private long[] loadEndOfMergeMarker(long deltaWALId, byte[] row) {
        long[] marker = UIO.bytesLongs(row);
        if (marker[EOM_VERSION_INDEX] != 1) {
            return null;
        }
        CRC32 crC32 = new CRC32();
        byte[] hintsAsBytes = UIO.longsBytes(marker);
        crC32.update(hintsAsBytes, 16, hintsAsBytes.length - 16); // 16 skips the version and checksum
        if (marker[EOM_CHECKSUM_INDEX] != crC32.getValue()) {
            return null;
        }
        if (deltaWALId > -1 && marker[EOM_DELTA_WAL_ID_INDEX] >= deltaWALId) {
            return null;
        }
        return marker;

    }

    public void writeHighwaterMarker(WALWriter rowWriter, WALHighwater highwater) throws Exception {
        synchronized (oneTransactionAtATimeLock) {
            rowWriter.writeHighwater(highwaterRowMarshaller.toBytes(highwater));
        }
    }

    public RowsChanged update(boolean generateRowsChanged,
        RowType rowType,
        long forceTxId,
        boolean forceApply,
        byte[] prefix,
        Commitable updates) throws Exception {

        if (!rowType.isPrimary()) {
            throw new IllegalArgumentException("rowType:" + rowType + " needs to be of type primary.");
        }

        Map<WALKey, WALValue> apply = new LinkedHashMap<>();
        List<KeyedTimestampId> removes = generateRowsChanged ? new ArrayList<>() : null;
        List<KeyedTimestampId> clobbers = generateRowsChanged ? new ArrayList<>() : null;

        List<byte[]> keys = new ArrayList<>();
        List<WALValue> values = new ArrayList<>();
        WALHighwater[] highwater = new WALHighwater[1];
        long updateVersion = orderIdProvider.nextId();

        updates.commitable(
            (_highwater) -> highwater[0] = _highwater,
            (transactionId, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                WALValue walValue = new WALValue(rowType, value, valueTimestamp, valueTombstoned, valueVersion != -1 ? valueVersion : updateVersion);
                Preconditions.checkArgument(valueTimestamp > 0, "Timestamp must be greater than zero");
                Preconditions.checkArgument(valueVersion > 0, "Version must be greater than zero");
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
            if (wali == null) {
                throw new IllegalStateException("Attempted to commit against a nonexistent index, it has likely been deleted");
            }
            RowsChanged rowsChanged = null;
            MutableLong indexCommittedFromTxId = new MutableLong(Long.MAX_VALUE);
            MutableLong indexCommittedUpToTxId = new MutableLong();

            if (!forceApply) {
                MutableInt i = new MutableInt(0);
                streamPointers(
                    stream -> {
                        for (int k = 0; k < keys.size(); k++) {
                            WALValue value = values.get(k);
                            boolean result = stream.stream(rowType,
                                prefix,
                                keys.get(k),
                                value.getValue(),
                                value.getTimestampId(),
                                value.getTombstoned(),
                                value.getVersion());
                            if (!result) {
                                return false;
                            }
                        }
                        return true;
                    },
                    (_rowType, _prefix, key, value, valueTimestamp, valueTombstone, valueVersion, pointerTimestamp, pointerTombstoned, pointerVersion,
                        pointerFp) -> {
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
                            if (generateRowsChanged) {
                                clobbers.add(keyedTimestampId);
                                if (valueTombstone && !pointerTombstoned) {
                                    removes.add(keyedTimestampId);
                                }
                            }
                        }
                        i.increment();
                        return true;
                    });
            }

            if (apply.isEmpty()) {
                if (generateRowsChanged) {
                    rowsChanged = new RowsChanged(versionedPartitionName, apply, removes, clobbers, -1, -1);
                }
            } else {
                int size = apply.size();
                long[] keyHighwaterTimestamps = findKeyHighwaterTimestamps();
                List<WALIndexable> indexables = new ArrayList<>(size);
                walTx.tx((io) -> {
                    int estimatedSizeInBytes = 0;
                    for (Entry<WALKey, WALValue> row : apply.entrySet()) {
                        byte[] value = row.getValue().getValue();
                        estimatedSizeInBytes += primaryRowMarshaller.maximumSizeInBytes(rowType,
                            row.getKey().sizeOfComposed(),
                            (value != null ? value.length : 0));
                    }
                    flush(rowType,
                        forceTxId,
                        size,
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
                        io,
                        highwater[0],
                        (rowTxId, _prefix, key, valueTimestamp, valueTombstoned, valueVersion, fp) -> {
                            minimize(oldestTimestamp, valueTimestamp);
                            minimize(oldestVersion, valueVersion);

                            if (valueTombstoned) {
                                minimize(oldestTombstonedTimestamp, valueTimestamp);
                                minimize(oldestTombstonedVersion, valueVersion);
                            }
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

                            mergeStripedKeyHighwaters(ix.prefix, ix.key, ix.valueTimestamp, keyHighwaterTimestamps);
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

                    if (generateRowsChanged) {
                        rowsChanged = new RowsChanged(versionedPartitionName,
                            apply,
                            removes,
                            clobbers,
                            indexCommittedFromTxId.longValue(),
                            indexCommittedUpToTxId.longValue());
                    }
                }
            }

            return rowsChanged;
        } finally {
            releaseOne();
        }
    }

    private static void minimize(AtomicLong existing, long value) {
        long existingValue = existing.get();
        if (existingValue == -1 || value < existingValue) {
            existing.set(value);
        }
    }

    private void flush(RowType rowType,
        long txId,
        int estimatedNumberOfRows,
        int estimatedSizeInBytes,
        RawRows rows,
        IndexableKeys indexableKeys,
        final MutableLong indexCommittedFromTxId,
        final MutableLong indexCommitedUpToTxId,
        WALWriter rowWriter,
        WALHighwater highwater,
        TxKeyPointerFpStream stream) throws Exception {

        synchronized (oneTransactionAtATimeLock) {
            if (txId == -1) {
                txId = orderIdProvider.nextId();
            }
            if (indexCommittedFromTxId.longValue() > txId) {
                indexCommittedFromTxId.setValue(txId);
            }
            try {
                if (indexCommitedUpToTxId.longValue() < txId) {
                    indexCommitedUpToTxId.setValue(txId);
                }
            } catch (NullPointerException e) {
                throw new IllegalStateException("Illogical NPE: " + (indexCommitedUpToTxId == null), e);
            }
            rowWriter.write(txId, rowType, estimatedNumberOfRows, estimatedSizeInBytes, rows, indexableKeys, stream, true, hardFsyncBeforeLeapBoundary);
            if (highwater != null) {
                writeHighwaterMarker(rowWriter, highwater);
            }
            highestTxId.set(indexCommitedUpToTxId.longValue());
        }
    }

    @Override
    public boolean rowScan(KeyValueStream keyValueStream) throws Exception {
        acquireOne();
        try {
            WALIndex wali = walIndex.get();
            return wali == null || wali.rowScan((prefix, key, timestamp, tombstoned, version, fp) -> {
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
            WALIndex wali = walIndex.get();
            if (wali == null) {
                return null;
            }
            TimestampedValue[] values = new TimestampedValue[1];
            wali.getPointer(prefix, key, (_prefix, _key, timestamp, tombstoned, version, fp) -> {
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
//
//    public WALPointer getPointer(byte[] prefix, byte[] key) throws Exception {
//        acquireOne();
//        try {
//            WALIndex wali = walIndex.get();
//            if (wali == null) {
//                return null;
//            }
//            WALPointer[] pointer = new WALPointer[1];
//            wali.getPointer(prefix, key, (_prefix, _key, timestamp, tombstoned, version, fp) -> {
//                if (fp != -1 && !tombstoned) {
//                    pointer[0] = new WALPointer(fp, timestamp, tombstoned, version);
//                }
//                return true;
//            });
//            return pointer[0];
//        } finally {
//            releaseOne();
//        }
//    }

    public boolean streamValues(byte[] prefix, UnprefixedWALKeys keys, KeyValueStream keyValueStream) throws Exception {
        acquireOne();
        try {
            WALIndex wali = walIndex.get();
            return wali == null || wali.getPointers(prefix, keys,
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
            WALIndex wali = walIndex.get();
            long[] keyHighwaterTimestamps = findKeyHighwaterTimestamps();
            return wali == null || wali.getPointers(
                indexStream -> keyValues.consume(
                    (rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                        long largestTimestamp = getLargestTimestampForKeyStripe(prefix, key, keyHighwaterTimestamps);
                        if (valueTimestamp > largestTimestamp) {
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
            WALIndex wali = walIndex.get();
            return wali != null && wali.containsKeys(prefix, keys, stream);
        } finally {
            releaseOne();
        }
    }

    private long[] findKeyHighwaterTimestamps() throws Exception {
        if (stripedKeyHighwaterTimestamps == null) {
            synchronized (this) {
                if (stripedKeyHighwaterTimestamps == null) {
                    if (versionedPartitionName.getPartitionName().isSystemPartition()) {
                        stripedKeyHighwaterTimestamps = new long[numKeyHighwaterStripes];
                    } else {
                        long[] compactKeyHighwaterTimestamps = new long[numKeyHighwaterStripes];
                        walTx.tx(io -> {
                            int[] total = {0};
                            io.reverseScan((rowFP, rowTxId, rowType1, row) -> {
                                if (rowType1 == RowType.end_of_merge) {
                                    long[] marker = loadEndOfMergeMarker(-1, row);
                                    if (marker == null) {
                                        throw new IllegalStateException("Invalid end of merge marker");
                                    }
                                    System.arraycopy(marker, EOM_HIGHWATER_STRIPES_OFFSET, compactKeyHighwaterTimestamps, 0, numKeyHighwaterStripes);
                                    return false;
                                } else if (!rowType1.isPrimary()) {
                                    total[0]++;
                                    LOG.warn("Expected end of merge marker but found type {} (total:{})", rowType1, total[0]);
                                    return true;
                                } else {
                                    //System.out.println("Expected EOM but found " + rowType1);
                                    throw new IllegalStateException("Expected end of merge marker but found type " + rowType1);
                                }
                            });
                            return null;
                        });
                        stripedKeyHighwaterTimestamps = compactKeyHighwaterTimestamps;
                    }
                }
            }
        }
        return stripedKeyHighwaterTimestamps;
    }

    private void clearKeyHighwaterTimestamps() {
        if (!versionedPartitionName.getPartitionName().isSystemPartition()) {
            stripedKeyHighwaterTimestamps = null;
        }
    }

    private void mergeStripedKeyHighwaters(byte[] prefix, byte[] key, long valueTimestamp, long[] keyHighwaterTimestamps) throws Exception {
        int highwaterTimestampIndex = Math.abs((Arrays.hashCode(prefix) ^ Arrays.hashCode(key)) % keyHighwaterTimestamps.length);
        keyHighwaterTimestamps[highwaterTimestampIndex] = Math.max(keyHighwaterTimestamps[highwaterTimestampIndex], valueTimestamp);
    }

    private long getLargestTimestampForKeyStripe(byte[] prefix, byte[] key, long[] keyHighwaterTimestamps) {
        int highwaterTimestampIndex = Math.abs((Arrays.hashCode(prefix) ^ Arrays.hashCode(key)) % keyHighwaterTimestamps.length);
        return keyHighwaterTimestamps[highwaterTimestampIndex];
    }

    //TODO replace with stream!
    private byte[] hydrateRowIndexValue(long indexFP) {
        try {
            return walTx.tx((io) -> io.readTypeByteTxIdAndRow(indexFP));
        } catch (Exception x) {

            long length;
            try {
                length = walTx.length();
            } catch (Exception e) {
                length = -1;
            }

            throw new RuntimeException(
                "Failed to hydrate for " + versionedPartitionName + " versionedPartitionName=" + versionedPartitionName + " size=" + length + " fp=" + indexFP,
                x);
        }
    }

    public boolean takeRowUpdatesSince(final long sinceTransactionId, final RowStream rowStream) throws Exception {
        if (sinceTransactionId >= highestTxId.get()) {
            return true;
        }
        acquireOne();
        try {
            long[] excessRows = new long[1];
            boolean readFromTransactionId = walIndex.get() == null || walTx.readFromTransactionId(sinceTransactionId,
                (offset, reader) -> reader.scan(offset,
                    false,
                    (rowPointer, rowTxId, rowType, row) -> {
                        if (rowType != RowType.system && rowTxId > sinceTransactionId) {
                            return rowStream.row(rowPointer, rowTxId, rowType, row);
                        } else {
                            excessRows[0]++;
                        }
                        return true;
                    }));
            amzaStats.takes.incrementAndGet();
            amzaStats.takeExcessRows.addAndGet(excessRows[0]);
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
            long[] excessRows = new long[1];
            WALIndex wali = walIndex.get();
            boolean readFromTransactionId = wali == null || walTx.tx(
                io -> io.read(
                    fpStream -> wali.takePrefixUpdatesSince(prefix, sinceTransactionId, (txId, fp) -> fpStream.stream(fp)),
                    (rowPointer, rowTxId, rowType, row) -> {
                        if (rowType != RowType.system && rowTxId > sinceTransactionId) {
                            return rowStream.row(rowPointer, rowTxId, rowType, row);
                        } else {
                            excessRows[0]++;
                        }
                        return true;
                    }));
            amzaStats.takes.incrementAndGet();
            amzaStats.takeExcessRows.addAndGet(excessRows[0]);
            return readFromTransactionId;
        } finally {
            releaseOne();
        }
    }

    public boolean takeAllRows(final RowStream rowStream) throws Exception {
        acquireOne();
        try {
            return walIndex.get() == null || walTx.readFromTransactionId(0, (offset, reader) -> reader.scan(offset, false, rowStream::row));
        } finally {
            releaseOne();
        }
    }

    public void updatedProperties(PartitionProperties partitionProperties) throws Exception {
        acquireOne();
        try {
            WALIndex wali = walIndex.get();
            if (wali != null) {
                wali.updatedProperties(partitionProperties.indexProperties);
            }
        } finally {
            releaseOne();
        }
    }

    public long count(WALKeyPointers keyPointers) throws Exception {
        acquireOne();
        try {
            WALIndex wali = walIndex.get();
            return wali == null ? 0 : wali.deltaCount(keyPointers) + keyCount.get();
        } finally {
            releaseOne();
        }
    }

    public long approximateCount() {
        acquireOne();
        try {
            WALIndex wali = walIndex.get();
            return wali == null ? 0 : keyCount.get();
        } finally {
            releaseOne();
        }
    }

    public long highestTxId() {
        return highestTxId.get();
    }

    public long mergedTxId() {
        return mergedTxId;
    }
}
