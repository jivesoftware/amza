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
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.shared.AckWaters;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPI;
import com.jivesoftware.os.amza.shared.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.scan.TxKeyValueStream;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.take.TakeResult;
import com.jivesoftware.os.amza.shared.wal.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.TimestampKeyValueStream;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Set;
import org.apache.commons.lang.mutable.MutableLong;

public class StripedPartition implements AmzaPartitionAPI {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaStats;
    private final OrderIdProvider orderIdProvider;
    private final WALUpdated walUpdated;
    private final RingMember ringMember;
    private final PartitionName partitionName;
    private final PartitionStripeProvider partitionStripeProvider;
    private final AckWaters ackWaters;
    private final AmzaRingStoreReader ringReader;

    public StripedPartition(AmzaStats amzaStats,
        OrderIdProvider orderIdProvider,
        WALUpdated walUpdated,
        RingMember ringMember,
        PartitionName partitionName,
        PartitionStripeProvider partitionStripeProvider,
        AckWaters ackWaters,
        AmzaRingStoreReader ringReader) {

        this.amzaStats = amzaStats;
        this.orderIdProvider = orderIdProvider;
        this.walUpdated = walUpdated;
        this.ringMember = ringMember;
        this.partitionName = partitionName;
        this.partitionStripeProvider = partitionStripeProvider;
        this.ackWaters = ackWaters;
        this.ringReader = ringReader;
    }

    public PartitionName getPartitionName() {
        return partitionName;
    }

    @Override
    public void commit(Commitable updates,
        int takeQuorum,
        long timeoutInMillis) throws Exception {

        long timestampId = orderIdProvider.nextId();
        partitionStripeProvider.txPartition(partitionName, (stripe, highwaterStorage) -> {
            RowsChanged commit = stripe.commit(highwaterStorage, partitionName, Optional.absent(), true, (highwaters, scan) -> {
                return updates.commitable(highwaters, (rowTxId, key, value, valueTimestamp, valueTombstone) -> {
                    long timestamp = valueTimestamp > 0 ? valueTimestamp : timestampId;
                    return scan.row(rowTxId, key, value, timestamp, valueTombstone);
                });
            }, walUpdated);
            amzaStats.direct(partitionName, commit.getApply().size(), commit.getOldestRowTxId());

            Set<RingMember> ringMembers = ringReader.getNeighboringRingMembers(partitionName.getRingName());

            if (takeQuorum > 0) {
                if (ringMembers.size() < takeQuorum) {
                    throw new FailedToAchieveQuorumException("There are an insufficent number of nodes to achieve desired take quorum:" + takeQuorum);
                } else {
                    LOG.debug("Awaiting quorum for {} ms", timeoutInMillis);
                    int takenBy = ackWaters.await(commit.getVersionedPartitionName(),
                        commit.getLargestCommittedTxId(),
                        ringMembers,
                        takeQuorum,
                        timeoutInMillis);
                    if (takenBy < takeQuorum) {
                        throw new FailedToAchieveQuorumException("Timed out attempting to achieve desired take quorum:" + takeQuorum + " got:" + takenBy);
                    }
                }
            }
            return null;
        });

    }

    @Override
    public boolean get(Iterable<WALKey> keys, TimestampKeyValueStream valuesStream) throws Exception {
        return partitionStripeProvider.txPartition(partitionName, (stripe, highwaterStorage) -> {
            return stripe.get(partitionName, (KeyValueStream stream) -> {
                for (WALKey key : keys) {
                    if (!stream.stream(key.getKey(), null, -1, false)) {
                        return false;
                    }
                }
                return true;
            }, valuesStream);
            
        });
    }

    @Override
    public void scan(WALKey from, WALKey to, Scan<TimestampedValue> scan) throws Exception {
        partitionStripeProvider.txPartition(partitionName, (stripe, highwaterStorage) -> {
            if (from == null && to == null) {
                stripe.rowScan(partitionName, (key, value, valueTimestamp, valueTombstone) -> valueTombstone || scan.row(-1, key,
                    new TimestampedValue(valueTimestamp, value)));
            } else {
                stripe.rangeScan(partitionName,
                    from == null ? new WALKey(new byte[0]) : from,
                    to,
                    (key, value, valueTimestamp, valueTombstone) -> valueTombstone || scan.row(-1, key,
                        new TimestampedValue(valueTimestamp, value)));
            }
            return null;
        });
    }

    @Override
    public TakeResult takeFromTransactionId(long transactionId, Highwaters highwaters, Scan<TimestampedValue> scan) throws Exception {
        return partitionStripeProvider.txPartition(partitionName, (stripe, highwaterStorage) -> {
            MutableLong lastTxId = new MutableLong(-1);
            WALHighwater tookToEnd = stripe.takeFromTransactionId(partitionName, transactionId, highwaterStorage, highwaters,
                (rowTxId, key, value, valueTimestamp, valueTombstone) -> {
                    if (valueTombstone || scan.row(rowTxId, key, new TimestampedValue(valueTimestamp, value))) {
                        if (rowTxId > lastTxId.longValue()) {
                            lastTxId.setValue(rowTxId);
                        }
                        return true;
                    }
                    return false;
                });
            return new TakeResult(ringMember, lastTxId.longValue(), tookToEnd);
        });
    }

    public WALHighwater takeAllFromTransactionId(PartitionName partitionName,
        long transactionId,
        Highwaters highwaters,
        TxKeyValueStream txKeyValueStream) throws Exception {

        return partitionStripeProvider.txPartition(partitionName, (stripe, highwaterStorage) -> {
            WALHighwater tookToEnd = stripe.takeFromTransactionId(partitionName, transactionId, highwaterStorage, highwaters,
                (rowTxId, key, value, valueTimestamp, valueTombstone) -> {
                    txKeyValueStream.row(rowTxId, key, value, valueTimestamp, valueTombstone);
                    return true;
                });
            return tookToEnd;
        });
    }

    @Override
    public long count() throws Exception {
        return partitionStripeProvider.txPartition(partitionName, (stripe, highwaterStorage) -> {
            return stripe.count(partitionName);
        });
    }

}
