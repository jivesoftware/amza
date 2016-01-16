package com.jivesoftware.os.amza.service;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jonathan.colt
 */
public class AckWaters {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AwaitNotify<VersionedPartitionName> awaitNotify;
    private final ConcurrentHashMap<RingMember, ConcurrentHashMap<VersionedPartitionName, LeadershipTokenAndTxId>> ackWaters = new ConcurrentHashMap<>();

    public AckWaters(int stripingLevel) {
        this.awaitNotify = new AwaitNotify<>(stripingLevel);
    }

    public void set(RingMember ringMember, VersionedPartitionName partitionName, Long txId, long leadershipToken) throws Exception {
        ConcurrentHashMap<VersionedPartitionName, LeadershipTokenAndTxId> partitionTxIds = ackWaters.computeIfAbsent(ringMember,
            (t) -> new ConcurrentHashMap<>());

        awaitNotify.notifyChange(partitionName, () -> {
            LeadershipTokenAndTxId result = partitionTxIds.compute(partitionName, (key, current) -> {
                if (current == null) {
                    return new LeadershipTokenAndTxId(leadershipToken, txId);
                } else {
                    if (txId <= current.txId && leadershipToken <= current.leadershipToken) {
                        return current;
                    }
                    return new LeadershipTokenAndTxId(Math.max(leadershipToken, current.leadershipToken), Math.max(txId, current.txId));
                }
            });

            return txId == result.txId || leadershipToken == result.leadershipToken;

        });
    }

    LeadershipTokenAndTxId get(RingMember ringMember, VersionedPartitionName partitionName) {
        ConcurrentHashMap<VersionedPartitionName, LeadershipTokenAndTxId> partitionTxIds = ackWaters.get(ringMember);
        if (partitionTxIds == null) {
            return null;
        }
        return partitionTxIds.get(partitionName);
    }

    static class LeadershipTokenAndTxId {

        final long leadershipToken;
        final long txId;

        LeadershipTokenAndTxId(long leadershipToken, long txId) {
            this.leadershipToken = leadershipToken;
            this.txId = txId;
        }
    }

    public int await(VersionedPartitionName partitionName,
        long desiredTxId,
        Collection<RingMember> takeRingMembers,
        int desiredTakeQuorum,
        long toMillis,
        long leadershipToken) throws Exception {

        RingMember[] ringMembers = takeRingMembers.toArray(new RingMember[takeRingMembers.size()]);
        int[] passed = new int[1];
        return awaitNotify.awaitChange(partitionName, () -> {
            for (int i = 0; i < ringMembers.length; i++) {
                RingMember ringMember = ringMembers[i];
                if (ringMember == null) {
                    continue;
                }
                LeadershipTokenAndTxId leadershipTokenAndTxId = get(ringMember, partitionName);
                if (leadershipToken > -1 && (leadershipTokenAndTxId != null && leadershipTokenAndTxId.leadershipToken > leadershipToken)) {
                    throw new FailedToAchieveQuorumException("Leader transitioning from " + leadershipToken + " to " + leadershipTokenAndTxId.leadershipToken);
                }
                if (leadershipTokenAndTxId != null && leadershipTokenAndTxId.txId >= desiredTxId) {
                    passed[0]++;
                    ringMembers[i] = null;
                }
                if (passed[0] >= desiredTakeQuorum) {
                    return Optional.of(passed[0]);
                }
            }
            return null;
        }, toMillis);

    }
}
