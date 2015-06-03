package com.jivesoftware.os.amza.client;

import com.jivesoftware.os.amza.shared.AmzaRegionAPI;
import com.jivesoftware.os.amza.shared.AmzaRegionAPI.TakeQuorum;
import com.jivesoftware.os.amza.shared.AmzaRegionAPI.TimestampedValue;
import com.jivesoftware.os.amza.shared.AmzaRegionAPIProvider;
import com.jivesoftware.os.amza.shared.region.RegionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.take.TakeCursors;
import com.jivesoftware.os.amza.shared.take.TakeResult;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 *
 * @author jonathan.colt
 */
public class Coolwhip {

    private final AmzaRegionAPIProvider provider;
    private final AmzaRegionAwaitQuorum awaitQuorum;

    public Coolwhip(AmzaRegionAPIProvider regionName,
        AmzaRegionAwaitQuorum awaitQuorum) {
        this.provider = regionName;
        this.awaitQuorum = awaitQuorum;
    }

    public void commit(RegionName regionName,
        Commitable<WALValue> commitable,
        int desiredTakeQuorum,
        long timeout,
        TimeUnit timeUnit) throws Exception {

        AmzaRegionAPI region = provider.getRegion(regionName);
        TakeQuorum takeQuorum = region.commit(commitable);
        if (desiredTakeQuorum > 0) {
            if (takeQuorum.getTakeOrderHosts().size() < desiredTakeQuorum) {
                throw new FailedToAchieveQuorumException("There are an insufficent number of nodes to achieve desired take quorum:" + desiredTakeQuorum);
            } else {
                awaitQuorum.await(regionName, takeQuorum, desiredTakeQuorum, timeUnit.toMillis(timeout));
            }
        }
    }

    public static class FailedToAchieveQuorumException extends Exception {

        public FailedToAchieveQuorumException(String message) {
            super(message);
        }

    }

    public void get(RegionName regionName, Iterable<byte[]> keys, Scan<TimestampedValue> valuesStream) throws Exception {
        // TODO impl quorum reads?
        provider.getRegion(regionName).get(keys, valuesStream);
    }

    public void scan(RegionName regionName, byte[] from, byte[] to, Scan<TimestampedValue> stream) throws Exception {
        // TODO impl WTF quorum scan? Really
        provider.getRegion(regionName).scan(from, to, stream);
    }

    private static final BiFunction<Long, Long, Long> maxMerge = Math::max;

    public TakeCursors takeFromTransactionId(RegionName regionName,
        long transactionId,
        Scan<TimestampedValue> scan) throws Exception {

        Map<RingMember, Long> ringMemberToMaxTxId = new HashMap<>();
        TakeResult takeResult = provider.getRegion(regionName).takeFromTransactionId(transactionId, (highwater) -> {
            for (WALHighwater.RingMemberHighwater memberHighwater : highwater.ringMemberHighwater) {
                ringMemberToMaxTxId.merge(memberHighwater.ringMember, memberHighwater.transactionId, maxMerge);
            }
        }, scan);

        if (takeResult.tookToEnd != null) {
            for (WALHighwater.RingMemberHighwater highwater : takeResult.tookToEnd.ringMemberHighwater) {
                ringMemberToMaxTxId.merge(highwater.ringMember, highwater.transactionId, maxMerge);
            }
        }
        ringMemberToMaxTxId.merge(takeResult.tookFrom, takeResult.lastTxId, maxMerge);

        List<TakeCursors.RingMemberCursor> cursors = new ArrayList<>();
        for (Map.Entry<RingMember, Long> entry : ringMemberToMaxTxId.entrySet()) {
            cursors.add(new TakeCursors.RingMemberCursor(entry.getKey(), entry.getValue()));
        }
        cursors.add(new TakeCursors.RingMemberCursor(takeResult.tookFrom, takeResult.lastTxId));
        return new TakeCursors(cursors);

    }

}
