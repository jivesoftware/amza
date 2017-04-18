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

import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.RangeScannable;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.KeyContainedStream;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.stats.AmzaStats.CompactionFamily;
import com.jivesoftware.os.amza.service.storage.WALStorage.TxTransitionToCompacted;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionStore implements RangeScannable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaStats;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final VersionedPartitionName versionedPartitionName;
    private final WALStorage walStorage;
    private final AtomicLong loadedAtDeltaWALId = new AtomicLong(Integer.MIN_VALUE);

    private volatile PartitionProperties properties;

    public PartitionStore(AmzaStats amzaStats,
        TimestampedOrderIdProvider orderIdProvider,
        VersionedPartitionName versionedPartitionName,
        WALStorage walStorage,
        PartitionProperties properties) {

        this.amzaStats = amzaStats;
        this.orderIdProvider = orderIdProvider;
        this.versionedPartitionName = versionedPartitionName;
        this.properties = properties;
        this.walStorage = walStorage;
    }

    public PartitionProperties getProperties() {
        return properties;
    }

    public WALStorage getWalStorage() {
        return walStorage;
    }

    public void load(
        File baseKey,
        long deltaWALId,
        long prevDeltaWALId,
        int stripe,
        ExecutorService executorService) throws Exception {

        if (checkIfLoaded(deltaWALId)) {
            return;
        }

        // load as a future so that initialization cannot be interrupted
        Future<Boolean> future = executorService.submit(() -> {
            if (checkIfLoaded(deltaWALId)) {
                return true;
            }
            synchronized (loadedAtDeltaWALId) {
                if (checkIfLoaded(deltaWALId)) {
                    return true;
                }
                PartitionProperties stackProperties = this.properties;
                boolean backwardScan = !versionedPartitionName.getPartitionName().isSystemPartition();
                boolean truncateToEndOfMergeMarker = deltaWALId != -1 && stackProperties.replicated;
                walStorage.load(amzaStats.loadIoStats, baseKey, deltaWALId, prevDeltaWALId, backwardScan, truncateToEndOfMergeMarker,
                    stackProperties.maxValueSizeInIndex, stripe);

                if (stackProperties.forceCompactionOnStartup) {
                    compactTombstone(
                        true,
                        baseKey,
                        baseKey,
                        stripe,
                        -1,
                        (transitionToCompacted) -> {
                            return transitionToCompacted.tx(() -> {
                                return null;
                            });
                        });
                }
                loadedAtDeltaWALId.set(deltaWALId);
                return true;
            }
        });
        future.get();
    }

    private boolean checkIfLoaded(long deltaWALId) {
        long loaded = loadedAtDeltaWALId.get();
        if (deltaWALId > -1) {
            if (loaded == -1) {
                throw new IllegalStateException("Partition was loaded without a delta before validation. attempted:" + deltaWALId);
            } else if (deltaWALId < loaded) {
                throw new IllegalStateException("Partition was loaded out of order. attempted:" + deltaWALId + " loaded:" + loaded);
            } else if (loaded != Integer.MIN_VALUE && deltaWALId >= loaded) {
                return true;
            }
        } else if (loaded != Integer.MIN_VALUE) {
            return true;
        }
        return false;
    }

    public boolean isSick() {
        return walStorage.isSick();
    }

    public void flush(boolean fsync) throws Exception {
        walStorage.flush(fsync);
    }

    @Override
    public boolean rowScan(KeyValueStream txKeyValueStream, boolean hydrateValues) throws Exception {
        return walStorage.rowScan(txKeyValueStream, hydrateValues);
    }

    @Override
    public boolean rangeScan(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey, KeyValueStream txKeyValueStream,
        boolean hydrateValues) throws Exception {
        return walStorage.rangeScan(fromPrefix, fromKey, toPrefix, toKey, txKeyValueStream, hydrateValues);
    }

    public void compactTombstone(
        boolean force,
        File fromBaseKey,
        File toBaseKey,
        int stripe,
        long disposalVersion,
        TxTransitionToCompacted transitionToCompacted) {
        // ageInMillis: 180 days
        // intervalMillis: 10 days
        // Do I have anything older than (180+10) days?
        // If so, then compact everything older than 180 days.
        long tombstoneCheckTimestamp = 0;
        long tombstoneCompactTimestamp = 0;
        long tombstoneCheckVersion = 0;
        long tombstoneCompactVersion = 0;
        long ttlCheckTimestamp = 0;
        long ttlCompactTimestamp = 0;
        long ttlCheckVersion = 0;
        long ttlCompactVersion = 0;
        PartitionProperties stackProperties = this.properties;
        if (stackProperties != null) {
            if (stackProperties.tombstoneTimestampAgeInMillis > 0) {
                tombstoneCheckTimestamp = getTimestampId(stackProperties.tombstoneTimestampAgeInMillis + stackProperties.tombstoneTimestampIntervalMillis);
                tombstoneCompactTimestamp = getTimestampId(stackProperties.tombstoneTimestampAgeInMillis);
            }
            if (stackProperties.tombstoneVersionAgeInMillis > 0) {
                tombstoneCheckVersion = getVersion(stackProperties.tombstoneVersionAgeInMillis + stackProperties.tombstoneVersionIntervalMillis);
                tombstoneCompactVersion = getVersion(stackProperties.tombstoneVersionAgeInMillis);
            }
            if (stackProperties.ttlTimestampAgeInMillis > 0) {
                ttlCheckTimestamp = getTimestampId(stackProperties.ttlTimestampAgeInMillis + stackProperties.ttlTimestampIntervalMillis);
                ttlCompactTimestamp = getTimestampId(stackProperties.ttlTimestampAgeInMillis);
            }
            if (stackProperties.ttlVersionAgeInMillis > 0) {
                ttlCheckVersion = getVersion(stackProperties.ttlVersionAgeInMillis + stackProperties.ttlVersionIntervalMillis);
                ttlCompactVersion = getVersion(stackProperties.ttlVersionAgeInMillis);
            }
        }

        try {
            if (force || walStorage.compactableTombstone(tombstoneCheckTimestamp, tombstoneCheckVersion, ttlCheckTimestamp, ttlCheckVersion, disposalVersion)) {
                String dir = fromBaseKey.toString();
                if (!fromBaseKey.equals(toBaseKey)) {
                    dir = " rebalance " + fromBaseKey + " to " + toBaseKey;
                }
                String name = versionedPartitionName.toString() + " " + dir + " stripe:" + stripe + " force:" + force;
                AmzaStats.CompactionStats compactionStats = amzaStats.beginCompaction(CompactionFamily.tombstone, name);
                try {
                    LOG.info("Compacting tombstoneTimestampId:{} tombstoneVersion:{} ttlTimestampId:{} ttlVersion:{} versionedPartitionName:{}",
                        tombstoneCompactTimestamp, tombstoneCompactVersion, ttlCompactTimestamp, ttlCompactVersion, versionedPartitionName);
                    boolean expectedEndOfMerge = !versionedPartitionName.getPartitionName().isSystemPartition();
                    walStorage.compactTombstone(amzaStats.compactTombstoneIoStats,
                        compactionStats,
                        fromBaseKey,
                        toBaseKey,
                        stackProperties.rowType,
                        tombstoneCompactTimestamp,
                        tombstoneCompactVersion,
                        ttlCompactTimestamp,
                        ttlCompactVersion,
                        disposalVersion,
                        stackProperties.maxValueSizeInIndex,
                        stripe,
                        expectedEndOfMerge,
                        transitionToCompacted);
                } finally {
                    compactionStats.finished();
                }
            } else {
                LOG.debug("Ignored tombstoneTimestampId:{} tombstoneVersion:{} ttlTimestampId:{} ttlVersion:{} versionedPartitionName:{}",
                    tombstoneCompactTimestamp, tombstoneCompactVersion, ttlCompactTimestamp, ttlCompactVersion, versionedPartitionName);
            }
        } catch (Exception x) {
            LOG.error("Failed to compact tombstones for partition: {}", new Object[] { versionedPartitionName }, x);
        }

    }

    private long getTimestampId(long timeAgoInMillis) {
        return System.currentTimeMillis() - timeAgoInMillis;
    }

    private long getVersion(long timeAgoInMillis) {
        return orderIdProvider.getApproximateId(System.currentTimeMillis() - timeAgoInMillis);
    }

    public TimestampedValue getTimestampedValue(byte[] prefix, byte[] key) throws Exception {
        return walStorage.getTimestampedValue(prefix, key);
    }

    public boolean streamValues(byte[] prefix, UnprefixedWALKeys keys, KeyValueStream stream) throws Exception {
        return walStorage.streamValues(prefix, keys, stream);
    }

    public boolean containsKey(byte[] prefix, byte[] key) throws Exception {
        boolean[] result = new boolean[1];
        walStorage.containsKeys(prefix, stream -> stream.stream(key), (_prefix, _key, contained, timestamp, version) -> {
            result[0] = contained;
            return true;
        });
        return result[0];
    }

    public boolean containsKeys(byte[] prefix, UnprefixedWALKeys keys, KeyContainedStream stream) throws Exception {
        return walStorage.containsKeys(prefix, keys, stream);
    }

    public boolean takeRowUpdatesSince(long transactionId, RowStream rowStream) throws Exception {
        return walStorage.takeRowUpdatesSince(amzaStats.takeIoStats, transactionId, rowStream);
    }

    public RowsChanged merge(boolean generateRowsChanged,
        PartitionProperties partitionProperties,
        long forceTxId,
        byte[] prefix,
        Commitable updates) throws Exception {
        return walStorage.update(amzaStats.mergeIoStats, generateRowsChanged, partitionProperties.rowType, forceTxId, true, prefix, updates);
    }

    public void updateProperties(PartitionProperties properties) throws Exception {
        this.properties = properties;
        walStorage.updatedProperties(properties);
    }

    public long highestTxId() {
        return walStorage.highestTxId();
    }

    public long mergedTxId() {
        return walStorage.mergedTxId();
    }

    public void delete(File baseKey) throws Exception {
        walStorage.delete(baseKey);
    }
}
