package com.jivesoftware.os.amza.service;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.KeyValueTimestampStream;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.TakeCursors;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.service.Partition.ScanRange;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jonathan.colt
 */
public class EmbeddedClientProvider { // Aka Partition Client Provider

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final PartitionProvider partitionProvider;

    public EmbeddedClientProvider(PartitionProvider partitionProvider) {
        this.partitionProvider = partitionProvider;
    }

    public EmbeddedClient getClient(PartitionName partitionName, CheckOnline checkOnline) {
        return new EmbeddedClient(partitionName, checkOnline);
    }

    public enum CheckOnline {
        never,
        once,
        always
    }

    public class EmbeddedClient { // Aka Partition Client

        private final PartitionName partitionName;
        private final CheckOnline checkOnline;
        private final AtomicBoolean requiresOnline;

        public EmbeddedClient(PartitionName partitionName, CheckOnline checkOnline) {
            this.partitionName = partitionName;
            this.checkOnline = checkOnline;
            this.requiresOnline = new AtomicBoolean(checkOnline != CheckOnline.never);
        }

        public void commit(Consistency consistency,
            byte[] prefix,
            ClientUpdates clientUpdates,
            long timeout,
            TimeUnit timeUnit) throws Exception {

            Partition partition = partitionProvider.getPartition(partitionName);
            partition.commit(consistency, prefix, clientUpdates, timeUnit.toMillis(timeout));
        }

        public void get(Consistency consistency, byte[] prefix, UnprefixedWALKeys keys, KeyValueTimestampStream valuesStream) throws Exception {
            // TODO impl quorum reads?
            partitionProvider.getPartition(partitionName).get(consistency, prefix, requiresOnline.get(), keys,
                (prefix1, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                    if (valueTimestamp == -1 || valueTombstoned) {
                        return valuesStream.stream(prefix1, key, null, -1, -1);
                    } else {
                        return valuesStream.stream(prefix1, key, value, valueTimestamp, valueVersion);
                    }
                });
            if (checkOnline == CheckOnline.once) {
                requiresOnline.set(false);
            }
        }

        public byte[] getValue(Consistency consistency, byte[] prefix, byte[] key) throws Exception {
            TimestampedValue timestampedValue = getTimestampedValue(consistency, prefix, key);
            return timestampedValue != null ? timestampedValue.getValue() : null;
        }

        public TimestampedValue getTimestampedValue(Consistency consistency, byte[] prefix, byte[] key) throws Exception {
            final TimestampedValue[] r = new TimestampedValue[1];
            get(consistency, prefix, stream -> stream.stream(key),
                (_prefix, _key, value, timestamp, version) -> {
                    if (timestamp != -1) {
                        r[0] = new TimestampedValue(timestamp, version, value);
                    }
                    return true;
                });
            return r[0];
        }

        public void scan(Iterable<ScanRange> ranges,
            KeyValueTimestampStream stream, boolean hydrateValues) throws Exception {
            partitionProvider.getPartition(partitionName).scan(ranges, hydrateValues, requiresOnline.get(),
                (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                    return valueTombstoned || stream.stream(prefix, key, value, valueTimestamp, valueVersion);
                });
            if (checkOnline == CheckOnline.once) {
                requiresOnline.set(false);
            }
        }

        public TakeCursors takeFromTransactionId(long transactionId, TxKeyValueStream stream) throws Exception {

            Map<RingMember, Long> ringMemberToMaxTxId = Maps.newHashMap();
            TakeResult takeResult = partitionProvider.getPartition(partitionName).takeFromTransactionId(transactionId,
                requiresOnline.get(),
                (highwater) -> {
                    for (WALHighwater.RingMemberHighwater memberHighwater : highwater.ringMemberHighwater) {
                        ringMemberToMaxTxId.merge(memberHighwater.ringMember, memberHighwater.transactionId, Math::max);
                    }
                },
                stream);
            if (checkOnline == CheckOnline.once) {
                requiresOnline.set(false);
            }

            boolean tookToEnd = false;
            if (takeResult.tookToEnd != null) {
                tookToEnd = true;
                for (WALHighwater.RingMemberHighwater highwater : takeResult.tookToEnd.ringMemberHighwater) {
                    ringMemberToMaxTxId.merge(highwater.ringMember, highwater.transactionId, Math::max);
                }
            }
            ringMemberToMaxTxId.merge(takeResult.tookFrom, takeResult.lastTxId, Math::max);

            List<TakeCursors.RingMemberCursor> cursors = new ArrayList<>();
            for (Map.Entry<RingMember, Long> entry : ringMemberToMaxTxId.entrySet()) {
                cursors.add(new TakeCursors.RingMemberCursor(entry.getKey(), entry.getValue()));
            }
            return new TakeCursors(cursors, tookToEnd);
        }

        public TakeCursors takeFromTransactionId(byte[] prefix, long transactionId, TxKeyValueStream stream) throws Exception {

            Map<RingMember, Long> ringMemberToMaxTxId = Maps.newHashMap();
            TakeResult takeResult = partitionProvider.getPartition(partitionName).takePrefixFromTransactionId(prefix,
                transactionId,
                requiresOnline.get(),
                (highwater) -> {
                    for (WALHighwater.RingMemberHighwater memberHighwater : highwater.ringMemberHighwater) {
                        ringMemberToMaxTxId.merge(memberHighwater.ringMember, memberHighwater.transactionId, Math::max);
                    }
                },
                stream);
            if (checkOnline == CheckOnline.once) {
                requiresOnline.set(false);
            }

            boolean tookToEnd = false;
            if (takeResult.tookToEnd != null) {
                tookToEnd = true;
                for (WALHighwater.RingMemberHighwater highwater : takeResult.tookToEnd.ringMemberHighwater) {
                    ringMemberToMaxTxId.merge(highwater.ringMember, highwater.transactionId, Math::max);
                }
            }
            ringMemberToMaxTxId.merge(takeResult.tookFrom, takeResult.lastTxId, Math::max);

            List<TakeCursors.RingMemberCursor> cursors = new ArrayList<>();
            for (Map.Entry<RingMember, Long> entry : ringMemberToMaxTxId.entrySet()) {
                cursors.add(new TakeCursors.RingMemberCursor(entry.getKey(), entry.getValue()));
            }
            return new TakeCursors(cursors, tookToEnd);
        }
    }

}
