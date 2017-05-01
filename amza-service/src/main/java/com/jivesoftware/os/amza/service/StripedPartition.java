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
package com.jivesoftware.os.amza.service;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.api.DeltaOverCapacityException;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.PrefixedKeyRanges;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream.TxResult;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.take.TakeCoordinator;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Set;

public class StripedPartition implements Partition {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaStats;
    private final OrderIdProvider orderIdProvider;
    private final VersionedPartitionProvider versionedPartitionProvider;
    private final WALUpdated walUpdated;
    private final RingMember ringMember;
    private final PartitionName partitionName;
    private final PartitionStripeProvider partitionStripeProvider;
    private final AckWaters ackWaters;
    private final AmzaRingStoreReader ringReader;
    private final TakeFullySystemReady systemReady;
    private final TakeCoordinator takeCoordinator;

    public StripedPartition(AmzaStats amzaStats,
        OrderIdProvider orderIdProvider,
        VersionedPartitionProvider versionedPartitionProvider,
        WALUpdated walUpdated,
        RingMember ringMember,
        PartitionName partitionName,
        PartitionStripeProvider partitionStripeProvider,
        AckWaters ackWaters,
        AmzaRingStoreReader ringReader,
        TakeFullySystemReady systemReady,
        TakeCoordinator takeCoordinator) {

        this.amzaStats = amzaStats;
        this.orderIdProvider = orderIdProvider;
        this.versionedPartitionProvider = versionedPartitionProvider;
        this.walUpdated = walUpdated;
        this.ringMember = ringMember;
        this.partitionName = partitionName;
        this.partitionStripeProvider = partitionStripeProvider;
        this.ackWaters = ackWaters;
        this.ringReader = ringReader;
        this.systemReady = systemReady;
        this.takeCoordinator = takeCoordinator;
    }

    public PartitionName getPartitionName() {
        return partitionName;
    }

    @Override
    public void commit(Consistency consistency,
        byte[] prefix,
        ClientUpdates updates,
        long timeoutInMillis) throws Exception {

        long end = System.currentTimeMillis() + timeoutInMillis;
        systemReady.await(timeoutInMillis);
        if (System.currentTimeMillis() > end) {
            throw new FailedToAchieveQuorumException("Timed out waiting for system ready");
        }

        PartitionProperties properties = versionedPartitionProvider.getProperties(partitionName);
        if (properties.requireConsistency && !properties.consistency.supportsWrites(consistency)) {
            throw new FailedToAchieveQuorumException("This partition has a minimum consistency of " + properties.consistency
                + " which does not support writes at consistency " + consistency);
        }

        Set<RingMember> neighbors = ringReader.getNeighboringRingMembers(partitionName.getRingName(), 0);
        int takeQuorum = consistency.quorum(neighbors.size());
        if (neighbors.size() < takeQuorum) {
            throw new FailedToAchieveQuorumException("There are an insufficent number of nodes to achieve desired take quorum:" + takeQuorum);
        }

        while (true) {
            try {
                long currentTime = System.currentTimeMillis();
                long version = orderIdProvider.nextId();
                partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
                    long leadershipToken = -1;
                    if (takeQuorum > 0) {
                        LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
                        if (consistency.requiresLeader() && (!livelyEndState.isOnline() || livelyEndState.getCurrentState() != State.leader)) {
                            throw new FailedToAchieveQuorumException("Leader has changed.");
                        }
                        leadershipToken = livelyEndState.getLeaderWaterline().getTimestamp();
                    }

                    RowsChanged commit = txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                        return partitionStripe.commit(highwaterStorage,
                            versionedAquarium,
                            true,
                            Optional.absent(),
                            true,
                            prefix,
                            (highwaters, stream) -> updates.updates((key, value, valueTimestamp, valueTombstone) -> {
                                long timestamp = valueTimestamp > 0 ? valueTimestamp : currentTime;
                                return stream.row(-1L, key, value, timestamp, valueTombstone, version);
                            }),
                            walUpdated);
                    });

                    if (takeQuorum > 0) {
                        long timeToWait = Math.max(0, end - System.currentTimeMillis());
                        LOG.debug("Awaiting quorum for {} ms", timeToWait);
                        int takenBy = 0;
                        if (timeToWait > 0) {
                            takenBy = ackWaters.await(versionedAquarium.getVersionedPartitionName(),
                                commit.getLargestCommittedTxId(),
                                neighbors,
                                takeQuorum,
                                timeToWait,
                                leadershipToken,
                                takeCoordinator);
                        }
                        if (takenBy < takeQuorum) {
                            throw new FailedToAchieveQuorumException(
                                "Timed out attempting to achieve desired take quorum:" + takeQuorum + " got:" + takenBy);
                        }
                    }
                    //TODO necessary? aquarium.tapTheGlass();

                    amzaStats.direct(partitionName, commit.getApply().size(), commit.getSmallestCommittedTxId());

                    return null;
                });
                break;
            } catch (DeltaOverCapacityException e) {
                long timeRemaining = end - System.currentTimeMillis();
                if (timeRemaining <= 0) {
                    throw e;
                }
                Thread.sleep(Math.min(timeRemaining, 1000L)); //TODO magic number
            }
        }

        long fsyncWaitInMillis = Math.max(end - System.currentTimeMillis(), 0);
        if (fsyncWaitInMillis > 0) {
            partitionStripeProvider.flush(partitionName, properties.durability, fsyncWaitInMillis);
        } else {
            throw new FailedToAchieveQuorumException("Timed out before commit achieved durability:" + properties.durability);
        }
    }

    private void checkReadConsistencySupport(Consistency consistency) throws Exception {
        PartitionProperties properties = versionedPartitionProvider.getProperties(partitionName);
        if (properties.requireConsistency && !properties.consistency.supportsReads(consistency)) {
            throw new FailedToAchieveQuorumException("This partition has a minimum consistency of " + properties.consistency
                + " which does not support reads at consistency " + consistency);
        }
    }

    @Override
    public boolean get(Consistency consistency, byte[] prefix, boolean requiresOnline, UnprefixedWALKeys keys, KeyValueStream stream) throws Exception {
        systemReady.await(0);
        checkReadConsistencySupport(consistency);
        return partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
            return txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                return partitionStripe.get(versionedAquarium, prefix, requiresOnline, keys, stream);
            });
        });
    }

    @Override
    public boolean scan(PrefixedKeyRanges ranges, boolean hydrateValues, boolean requiresOnline, KeyValueStream stream) throws Exception {

        systemReady.await(0);
        return partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
            return txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                return ranges.consume((fromPrefix, fromKey, toPrefix, toKey) -> {
                    if (fromKey == null && toKey == null) {
                        partitionStripe.rowScan(versionedAquarium, stream, hydrateValues, requiresOnline);
                    } else {
                        partitionStripe.rangeScan(versionedAquarium,
                            fromPrefix,
                            fromKey,
                            toPrefix,
                            toKey,
                            hydrateValues,
                            requiresOnline,
                            stream);
                    }
                    return true;
                });
            });
        });
    }

    @Override
    public TakeResult takeFromTransactionId(long txId,
        boolean requiresOnline,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception {

        systemReady.await(0);
        return takeFromTransactionIdInternal(false, null, txId, requiresOnline, highwaters, stream);
    }

    @Override
    public TakeResult takePrefixFromTransactionId(byte[] prefix,
        long txId,
        boolean requiresOnline,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception {

        Preconditions.checkNotNull(prefix, "Must specify a prefix");
        systemReady.await(0);
        return takeFromTransactionIdInternal(true, prefix, txId, requiresOnline, highwaters, stream);
    }

    private TakeResult takeFromTransactionIdInternal(boolean usePrefix,
        byte[] takePrefix,
        long txId,
        boolean requiresOnline,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception {

        return partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
            long[] lastTxId = { -1 };
            TxResult[] done = new TxResult[1];
            TxKeyValueStream txKeyValueStream = (rowTxId, prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                if (done[0] != null && rowTxId > lastTxId[0]) {
                    // streamed to end of txId
                    return done[0];
                }

                if (done[0] != null) {
                    if (done[0].isAccepted()) {
                        // ignore result; lastTxId is unchanged
                        stream.stream(rowTxId, prefix, key, value, valueTimestamp, valueTombstone, valueVersion);
                    }
                } else {
                    TxResult result = stream.stream(rowTxId, prefix, key, value, valueTimestamp, valueTombstone, valueVersion);
                    if (result.isAccepted()) {
                        if (rowTxId > lastTxId[0]) {
                            lastTxId[0] = rowTxId;
                        }
                    }
                    if (!result.wantsMore()) {
                        if (result.isAccepted()) {
                            // stream to end of txId
                            done[0] = result;
                        } else {
                            // reject entire txId
                            return result;
                        }
                    }
                }
                return TxResult.MORE;
            };

            WALHighwater highwater = txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                if (usePrefix) {
                    return partitionStripe.takeFromTransactionId(versionedAquarium,
                        takePrefix,
                        txId,
                        requiresOnline,
                        highwaterStorage,
                        highwaters,
                        txKeyValueStream);
                } else {
                    return partitionStripe.takeFromTransactionId(amzaStats.takeIoStats,
                        versionedAquarium,
                        txId,
                        requiresOnline,
                        highwaterStorage,
                        highwaters,
                        txKeyValueStream);
                }
            });
            return new TakeResult(ringMember, lastTxId[0], highwater);
        });
    }

    @Override
    public long count() throws Exception {
        return partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
            return txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                return partitionStripe.count(versionedAquarium);
            });
        });
    }

    @Override
    public long approximateCount() throws Exception {
        return partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
            return txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                return partitionStripe.approximateCount(versionedAquarium);
            });
        });
    }

    @Override
    public long highestTxId() throws Exception {
        return partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
            return txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                return partitionStripe.highestTxId(versionedAquarium.getVersionedPartitionName());
            });
        });
    }

    @Override
    public LivelyEndState livelyEndState() throws Exception {
        return partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
            return versionedAquarium.getLivelyEndState();
        });
    }
}
