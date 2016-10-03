package com.jivesoftware.os.amza.service;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.take.TakeCoordinator;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * @author jonathan.colt
 */
public class AckWaters {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaStats;
    private final AwaitNotify<VersionedPartitionName> awaitNotify;
    private final boolean verboseLogTimeouts;
    private final ConcurrentHashMap<RingMember, ConcurrentHashMap<VersionedPartitionName, LeadershipTokenAndTxId>> ackWaters = new ConcurrentHashMap<>();

    public AckWaters(AmzaStats amzaStats, int stripingLevel, boolean verboseLogTimeouts) {
        this.amzaStats = amzaStats;
        this.awaitNotify = new AwaitNotify<>(stripingLevel);
        this.verboseLogTimeouts = verboseLogTimeouts;
    }

    public void set(RingMember ringMember, VersionedPartitionName partitionName, long txId, long leadershipToken) throws Exception {
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

    public int await(VersionedPartitionName versionedPartitionName,
        long desiredTxId,
        Collection<RingMember> takeRingMembers,
        int desiredTakeQuorum,
        long toMillis,
        long leadershipToken,
        TakeCoordinator takeCoordinator) throws Exception {

        RingMember[] ringMembers = takeRingMembers.toArray(new RingMember[takeRingMembers.size()]);
        int[] passed = new int[1];
        long[] tookToTxId = new long[ringMembers.length];
        Arrays.fill(tookToTxId, -1);
        try {
            long start = System.currentTimeMillis();
            Integer quorum = awaitNotify.awaitChange(versionedPartitionName, () -> {
                for (int i = 0; i < ringMembers.length; i++) {
                    RingMember ringMember = ringMembers[i];
                    if (ringMember == null) {
                        continue;
                    }
                    LeadershipTokenAndTxId leadershipTokenAndTxId = get(ringMember, versionedPartitionName);
                    if (leadershipToken > -1 && (leadershipTokenAndTxId != null && leadershipTokenAndTxId.leadershipToken > leadershipToken)) {
                        throw new FailedToAchieveQuorumException(
                            "Leader transitioning from " + leadershipToken + " to " + leadershipTokenAndTxId.leadershipToken);
                    }
                    if (leadershipTokenAndTxId != null && leadershipTokenAndTxId.txId >= desiredTxId) {
                        passed[0]++;
                        ringMembers[i] = null;
                    }
                    tookToTxId[i] = leadershipTokenAndTxId != null ? leadershipTokenAndTxId.txId : -1;
                    if (passed[0] >= desiredTakeQuorum) {
                        return Optional.of(passed[0]);
                    }
                }
                return null;
            }, toMillis);
            amzaStats.quorums(versionedPartitionName.getPartitionName(), 1, System.currentTimeMillis() - start);
            return quorum;
        } catch (TimeoutException e) {
            if (verboseLogTimeouts) {
                StringBuilder buf = new StringBuilder();
                takeCoordinator.streamTookLatencies(versionedPartitionName,
                    (ringMember, lastOfferedTxId, category, tooSlowTxId, takeSessionId, online, steadyState, lastOfferedMillis, lastTakenMillis,
                        lastCategoryCheckMillis) -> {
                        buf.append('\n').append(String.format(
                            "- member:%s lastOfferedTxId:%s category:%s tooSlowTxId:%s takeSessionId:%s online:%s " +
                                "steadyState:%s lastOfferedMillis:%s lastTakenMillis:%s, lastCategoryCheckMillis:%s",
                            ringMember, lastOfferedTxId, category, tooSlowTxId, takeSessionId, online,
                            steadyState, lastOfferedMillis, lastTakenMillis, lastCategoryCheckMillis));
                        return true;
                    });
                LOG.warn("Failed to achieve quorum for partition:{} desiredTxId:{} desiredTakeQuorum:{} passed:{} leadershipToken:{} tookToTxId:{} details:{}",
                    versionedPartitionName, desiredTxId, desiredTakeQuorum, passed[0], leadershipToken, Arrays.toString(tookToTxId), buf);
            }
            amzaStats.quorumTimeouts(versionedPartitionName.getPartitionName(), 1);
            throw e;
        }
    }

    public interface MemberTxIdStream {
        boolean stream(RingMember member, long txId) throws Exception;
    }

    public boolean streamPartitionTxIds(VersionedPartitionName versionedPartitionName, MemberTxIdStream stream) throws Exception {
        for (Entry<RingMember, ConcurrentHashMap<VersionedPartitionName, LeadershipTokenAndTxId>> entry : ackWaters.entrySet()) {
            RingMember member = entry.getKey();
            ConcurrentHashMap<VersionedPartitionName, LeadershipTokenAndTxId> partitionTxIds = entry.getValue();
            LeadershipTokenAndTxId leadershipTokenAndTxId = partitionTxIds.get(versionedPartitionName);
            if (leadershipTokenAndTxId != null) {
                if (!stream.stream(member, leadershipTokenAndTxId.txId)) {
                    return false;
                }
            }
        }
        return true;
    }
}
