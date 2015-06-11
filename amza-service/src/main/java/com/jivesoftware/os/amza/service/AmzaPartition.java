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
import com.jivesoftware.os.amza.service.replication.PartitionStripe;
import com.jivesoftware.os.amza.shared.AckWaters;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPI;
import com.jivesoftware.os.amza.shared.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.take.TakeResult;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Set;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

public class AmzaPartition implements AmzaPartitionAPI {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaStats;
    private final OrderIdProvider orderIdProvider;
    private final RingMember ringMember;
    private final PartitionName partitionName;
    private final PartitionStripe partitionStripe;
    private final HighwaterStorage highwaterStorage;
    private final AckWaters ackWaters;
    private final AmzaRingReader ringReader;

    public AmzaPartition(AmzaStats amzaStats,
        OrderIdProvider orderIdProvider,
        RingMember ringMember,
        PartitionName partitionName,
        PartitionStripe partitionStripe,
        HighwaterStorage highwaterStorage,
        AckWaters ackWaters,
        AmzaRingReader ringReader) {

        this.amzaStats = amzaStats;
        this.orderIdProvider = orderIdProvider;
        this.ringMember = ringMember;
        this.partitionName = partitionName;
        this.partitionStripe = partitionStripe;
        this.highwaterStorage = highwaterStorage;
        this.ackWaters = ackWaters;
        this.ringReader = ringReader;
    }

    public PartitionName getPartitionName() {
        return partitionName;
    }

    @Override
    public void commit(Commitable<WALValue> updates,
        int takeQuorum,
        long timeoutInMillis) throws Exception {

        long timestampId = orderIdProvider.nextId();
        RowsChanged commit = partitionStripe.commit(partitionName, Optional.absent(), true, (highwaters, scan) -> {
            updates.commitable(highwaters, (rowTxId, key, scanned) -> {
                WALValue value = scanned.getTimestampId() > 0 ? scanned : new WALValue(scanned.getValue(), timestampId, scanned.getTombstoned());
                return scan.row(rowTxId, key, value);
            });
        });

        Set<RingMember> ringMembers = ringReader.getNeighboringRingMembers(partitionName.getRingName());

        if (takeQuorum > 0) {
            if (ringMembers.size() < takeQuorum) {
                throw new FailedToAchieveQuorumException("There are an insufficent number of nodes to achieve desired take quorum:" + takeQuorum);
            } else {
                LOG.debug("Awaiting quorum for {} ms", timeoutInMillis);
                int takenBy = ackWaters.await(commit.getVersionedPartitionName(), commit.getLargestCommittedTxId(), ringMembers, takeQuorum, timeoutInMillis);
                if (takenBy < takeQuorum) {
                    throw new FailedToAchieveQuorumException("Timed out attempting to achieve desired take quorum:" + takeQuorum + " got:" + takenBy);
                }
            }
        }
    }

    @Override
    public void get(Iterable<WALKey> keys, Scan<TimestampedValue> valuesStream) throws Exception {
        for (WALKey walKey : keys) {
            WALValue got = partitionStripe.get(partitionName, walKey); // TODO Hmmm add a multi get?
            valuesStream.row(-1, walKey, got == null || got.getTombstoned() ? null : got.toTimestampedValue());
        }
    }

    @Override
    public void scan(WALKey from, WALKey to, Scan<TimestampedValue> scan) throws Exception {
        if (from == null && to == null) {
            partitionStripe.rowScan(partitionName, (rowTxId, key, scanned) -> scanned.getTombstoned() || scan.row(rowTxId, key, scanned.toTimestampedValue()));
        } else {
            partitionStripe.rangeScan(partitionName,
                from == null ? new WALKey(new byte[0]) : from,
                to,
                (rowTxId, key, scanned) -> scanned.getTombstoned() || scan.row(rowTxId, key, scanned.toTimestampedValue()));
        }
    }

    @Override
    public TakeResult takeFromTransactionId(long transactionId, Highwaters highwaters, Scan<TimestampedValue> scan) throws Exception {
        final MutableLong lastTxId = new MutableLong(-1);
        WALHighwater tookToEnd = partitionStripe.takeFromTransactionId(partitionName, transactionId, highwaterStorage, highwaters, (rowTxId, key, value) -> {
            if (value.getTombstoned() || scan.row(rowTxId, key, value.toTimestampedValue())) {
                if (rowTxId > lastTxId.longValue()) {
                    lastTxId.setValue(rowTxId);
                }
                return true;
            }
            return false;
        });
        return new TakeResult(ringMember, lastTxId.longValue(), tookToEnd);
    }

    //  Use for testing
    public boolean compare(final AmzaPartition amzaPartition) throws Exception {
        final MutableInt compared = new MutableInt(0);
        final MutableBoolean passed = new MutableBoolean(true);
        amzaPartition.scan(null, null, (txid, key, value) -> {
            try {
                compared.increment();

                WALValue timestampedValue = partitionStripe.get(partitionName, key);
                String comparing = partitionName.getRingName() + ":" + partitionName.getPartitionName()
                    + " to " + amzaPartition.partitionName.getRingName() + ":" + amzaPartition.partitionName.getPartitionName() + "\n";

                if (timestampedValue == null) {
                    System.out.println("INCONSISTENCY: " + comparing + " key:null"
                        + " != " + value.getTimestampId()
                        + "' \n" + timestampedValue + " vs " + value);
                    passed.setValue(false);
                    return false;
                }
                if (value.getTimestampId() != timestampedValue.getTimestampId()) {
                    System.out.println("INCONSISTENCY: " + comparing + " timstamp:'" + timestampedValue.getTimestampId()
                        + "' != '" + value.getTimestampId()
                        + "' \n" + timestampedValue + " vs " + value);
                    passed.setValue(false);
                    System.out.println("----------------------------------");

                    return false;
                }
                if (value.getValue() == null && timestampedValue.getValue() != null) {
                    System.out.println("INCONSISTENCY: " + comparing + " null values:" + timestampedValue.getTombstoned()
                        + " != '" + value
                        + "' \n" + timestampedValue + " vs " + value);
                    passed.setValue(false);
                    return false;
                }
                if (value.getValue() != null && !Arrays.equals(value.getValue(), timestampedValue.getValue())) {
                    System.out.println("INCONSISTENCY: " + comparing + " value:'" + Arrays.toString(timestampedValue.getValue())
                        + "' != '" + Arrays.toString(value.getValue())
                        + "' aClass:" + timestampedValue.getValue().getClass()
                        + "' bClass:" + value.getValue().getClass()
                        + "' \n" + timestampedValue + " vs " + value);
                    passed.setValue(false);
                    return false;
                }
                return true;
            } catch (Exception x) {
                throw new RuntimeException("Failed while comparing", x);
            }
        });

        System.out.println("partition:" + amzaPartition.partitionName.getPartitionName() + " compared:" + compared + " keys");
        return passed.booleanValue();
    }

    public long count() throws Exception {
        return partitionStripe.count(partitionName);
    }

}
