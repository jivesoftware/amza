package com.jivesoftware.os.amza.client;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPI;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPI.TimestampedValue;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPIProvider;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.take.TakeCursors;
import com.jivesoftware.os.amza.shared.take.TakeResult;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan.colt
 */
public class AmzaKretr {  // Aka Partition Client

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaPartitionAPIProvider partitionAPIProvider;

    public AmzaKretr(AmzaPartitionAPIProvider partitionAPIProvider) {
        this.partitionAPIProvider = partitionAPIProvider;
    }

    public void commit(PartitionName partitionName,
        Commitable<WALValue> commitable,
        int desiredTakeQuorum,
        long timeout,
        TimeUnit timeUnit) throws Exception {

        AmzaPartitionAPI partition = partitionAPIProvider.getPartition(partitionName);
        partition.commit(commitable, desiredTakeQuorum, timeUnit.toMillis(timeout));
    }

    public void get(PartitionName partitionName, Iterable<byte[]> keys, Scan<TimestampedValue> valuesStream) throws Exception {
        // TODO impl quorum reads?
        partitionAPIProvider.getPartition(partitionName).get(keys, valuesStream);
    }

    public void scan(PartitionName partitionName, byte[] from, byte[] to, Scan<TimestampedValue> stream) throws Exception {
        // TODO impl WTF quorum scan? Really
        partitionAPIProvider.getPartition(partitionName).scan(from, to, stream);
    }

    public TakeCursors takeFromTransactionId(PartitionName partitionName,
        long transactionId,
        Scan<TimestampedValue> scan) throws Exception {

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
        cursors.add(new TakeCursors.RingMemberCursor(takeResult.tookFrom, takeResult.lastTxId));
        return new TakeCursors(cursors);

    }

}
