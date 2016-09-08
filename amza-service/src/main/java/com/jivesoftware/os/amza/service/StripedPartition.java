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
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.KeyValueTimestampStream;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
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
    private final AmzaSystemReady systemReady;

    public StripedPartition(AmzaStats amzaStats,
        OrderIdProvider orderIdProvider,
        VersionedPartitionProvider versionedPartitionProvider,
        WALUpdated walUpdated,
        RingMember ringMember,
        PartitionName partitionName,
        PartitionStripeProvider partitionStripeProvider,
        AckWaters ackWaters,
        AmzaRingStoreReader ringReader,
        AmzaSystemReady systemReady) {

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
        long remainingTimeInMillis = Math.max(end - System.currentTimeMillis(), 0);

        PartitionProperties properties = versionedPartitionProvider.getProperties(partitionName);
        if (properties.requireConsistency && !properties.consistency.supportsWrites(consistency)) {
            throw new FailedToAchieveQuorumException("This partition has a minimum consistency of " + properties.consistency
                + " which does not support writes at consistency " + consistency);
        }

        Set<RingMember> neighbors = ringReader.getNeighboringRingMembers(partitionName.getRingName());
        int takeQuorum = consistency.quorum(neighbors.size());
        if (neighbors.size() < takeQuorum) {
            throw new FailedToAchieveQuorumException("There are an insufficent number of nodes to achieve desired take quorum:" + takeQuorum);
        }

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
                LOG.debug("Awaiting quorum for {} ms", remainingTimeInMillis);
                int takenBy = ackWaters.await(versionedAquarium.getVersionedPartitionName(),
                    commit.getLargestCommittedTxId(),
                    neighbors,
                    takeQuorum,
                    remainingTimeInMillis,
                    leadershipToken);
                if (takenBy < takeQuorum) {
                    throw new FailedToAchieveQuorumException(
                        "Timed out attempting to achieve desired take quorum:" + takeQuorum + " got:" + takenBy);
                }
            }
            //TODO necessary? aquarium.tapTheGlass();

            amzaStats.direct(partitionName, commit.getApply().size(), commit.getSmallestCommittedTxId());

            return null;
        });

        // TODO consider creating stripes per durability
        long fsyncWaitInMillis = Math.max(end - System.currentTimeMillis(), 0);
        partitionStripeProvider.flush(partitionName, properties.durability, fsyncWaitInMillis);
    }

    private void checkReadConsistencySupport(Consistency consistency) throws Exception {
        PartitionProperties properties = versionedPartitionProvider.getProperties(partitionName);
        if (properties.requireConsistency && !properties.consistency.supportsReads(consistency)) {
            throw new FailedToAchieveQuorumException("This partition has a minimum consistency of " + properties.consistency
                + " which does not support reads at consistency " + consistency);
        }
    }

    @Override
    public boolean get(Consistency consistency, byte[] prefix, UnprefixedWALKeys keys, KeyValueStream stream) throws Exception {
        systemReady.await(0);
        checkReadConsistencySupport(consistency);
        return partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
            return txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                return partitionStripe.get(versionedAquarium, prefix, keys, stream);
            });
        });
    }

    @Override
    public boolean scan(Iterable<ScanRange> ranges, KeyValueStream stream, boolean hydrateValues) throws Exception {

        systemReady.await(0);
        return partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
            return txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                for (ScanRange range : ranges) {
                    if (range.fromKey == null && range.toKey == null) {
                        partitionStripe.rowScan(versionedAquarium, stream, hydrateValues);
                    } else {
                        partitionStripe.rangeScan(versionedAquarium,
                            range.fromPrefix,
                            range.fromKey,
                            range.toPrefix,
                            range.toKey,
                            stream,
                            hydrateValues);
                    }
                }

                return true;
            });
        });
    }

    @Override
    public TakeResult takeFromTransactionId(long txId,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception {

        systemReady.await(0);
        return takeFromTransactionIdInternal(false, null, txId, highwaters, stream);
    }

    @Override
    public TakeResult takePrefixFromTransactionId(byte[] prefix,
        long txId,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception {

        Preconditions.checkNotNull(prefix, "Must specify a prefix");
        systemReady.await(0);
        return takeFromTransactionIdInternal(true, prefix, txId, highwaters, stream);
    }

    private TakeResult takeFromTransactionIdInternal(boolean usePrefix,
        byte[] takePrefix,
        long txId,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception {

        return partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
            long[] lastTxId = { -1 };
            boolean[] done = { false };
            TxKeyValueStream txKeyValueStream = (rowTxId, prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                if (done[0] && rowTxId > lastTxId[0]) {
                    return false;
                }

                done[0] |= !stream.stream(rowTxId, prefix, key, value, valueTimestamp, valueTombstone, valueVersion);
                if (rowTxId > lastTxId[0]) {
                    lastTxId[0] = rowTxId;
                }
                return true;
            };

            WALHighwater highwater = txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                if (usePrefix) {
                    return partitionStripe.takeFromTransactionId(versionedAquarium, takePrefix, txId, highwaterStorage, highwaters, txKeyValueStream);
                } else {
                    return partitionStripe.takeFromTransactionId(versionedAquarium, txId, highwaterStorage, highwaters, txKeyValueStream);
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
                return partitionStripe.highestAquariumTxId(versionedAquarium);
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
