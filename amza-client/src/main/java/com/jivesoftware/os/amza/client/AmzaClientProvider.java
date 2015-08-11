package com.jivesoftware.os.amza.client;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPI;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPIProvider;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.stream.TimestampKeyValueStream;
import com.jivesoftware.os.amza.shared.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.shared.take.TakeCursors;
import com.jivesoftware.os.amza.shared.take.TakeResult;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan.colt
 */
public class AmzaClientProvider { // Aka Partition Client Provider

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaPartitionAPIProvider partitionAPIProvider;

    public AmzaClientProvider(AmzaPartitionAPIProvider partitionAPIProvider) {
        this.partitionAPIProvider = partitionAPIProvider;
    }

    public AmzaClient getClient(PartitionName partitionName) {
        return new AmzaClient(partitionName);
    }

    public class AmzaClient { // Aka Partition Client

        private final PartitionName partitionName;

        public AmzaClient(PartitionName partitionName) {
            this.partitionName = partitionName;
        }

        public void commit(byte[] prefix,
            Commitable commitable,
            int desiredTakeQuorum,
            long timeout,
            TimeUnit timeUnit) throws Exception {

            AmzaPartitionAPI partition = partitionAPIProvider.getPartition(partitionName);
            partition.commit(prefix, commitable, desiredTakeQuorum, timeUnit.toMillis(timeout));
        }

        public void get(byte[] prefix, UnprefixedWALKeys keys, TimestampKeyValueStream valuesStream) throws Exception {
            // TODO impl quorum reads?
            partitionAPIProvider.getPartition(partitionName).get(prefix, keys, valuesStream);
        }

        public byte[] getValue(byte[] prefix, byte[] key) throws Exception {
            TimestampedValue timestampedValue = getTimestampedValue(prefix, key);
            return timestampedValue != null ? timestampedValue.getValue() : null;
        }

        public TimestampedValue getTimestampedValue(byte[] prefix, byte[] key) throws Exception {
            final TimestampedValue[] r = new TimestampedValue[1];
            get(prefix, stream -> stream.stream(key),
                (_prefix, _key, value, timestamp) -> {
                    r[0] = new TimestampedValue(timestamp, value);
                    return true;
                });
            return r[0];
        }

        public void scan(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey, Scan<TimestampedValue> stream) throws Exception {
            // TODO impl WTF quorum scan? Really
            partitionAPIProvider.getPartition(partitionName).scan(fromPrefix, fromKey, toPrefix, toKey, stream);
        }

        public TakeCursors takeFromTransactionId(long transactionId, Scan<TimestampedValue> scan) throws Exception {

            Map<RingMember, Long> ringMemberToMaxTxId = Maps.newHashMap();
            TakeResult takeResult = partitionAPIProvider.getPartition(partitionName).takeFromTransactionId(transactionId, (highwater) -> {
                for (WALHighwater.RingMemberHighwater memberHighwater : highwater.ringMemberHighwater) {
                    ringMemberToMaxTxId.merge(memberHighwater.ringMember, memberHighwater.transactionId, Math::max);
                }
            }, scan);

            if (takeResult.tookToEnd != null) {
                for (WALHighwater.RingMemberHighwater highwater : takeResult.tookToEnd.ringMemberHighwater) {
                    ringMemberToMaxTxId.merge(highwater.ringMember, highwater.transactionId, Math::max);
                }
            }
            ringMemberToMaxTxId.merge(takeResult.tookFrom, takeResult.lastTxId, Math::max);

            List<TakeCursors.RingMemberCursor> cursors = new ArrayList<>();
            for (Map.Entry<RingMember, Long> entry : ringMemberToMaxTxId.entrySet()) {
                cursors.add(new TakeCursors.RingMemberCursor(entry.getKey(), entry.getValue()));
            }
            return new TakeCursors(cursors);
        }

        public TakeCursors takeFromTransactionId(byte[] prefix, long transactionId, Scan<TimestampedValue> scan) throws Exception {

            Map<RingMember, Long> ringMemberToMaxTxId = Maps.newHashMap();
            TakeResult takeResult = partitionAPIProvider.getPartition(partitionName).takeFromTransactionId(prefix, transactionId, (highwater) -> {
                for (WALHighwater.RingMemberHighwater memberHighwater : highwater.ringMemberHighwater) {
                    ringMemberToMaxTxId.merge(memberHighwater.ringMember, memberHighwater.transactionId, Math::max);
                }
            }, scan);

            if (takeResult.tookToEnd != null) {
                for (WALHighwater.RingMemberHighwater highwater : takeResult.tookToEnd.ringMemberHighwater) {
                    ringMemberToMaxTxId.merge(highwater.ringMember, highwater.transactionId, Math::max);
                }
            }
            ringMemberToMaxTxId.merge(takeResult.tookFrom, takeResult.lastTxId, Math::max);

            List<TakeCursors.RingMemberCursor> cursors = new ArrayList<>();
            for (Map.Entry<RingMember, Long> entry : ringMemberToMaxTxId.entrySet()) {
                cursors.add(new TakeCursors.RingMemberCursor(entry.getKey(), entry.getValue()));
            }
            return new TakeCursors(cursors);
        }
    }

}
