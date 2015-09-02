package com.jivesoftware.os.amza.aquarium;

import com.google.common.collect.Sets;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class Liveliness {

    private final LivelinessStorage livelinessStorage;
    private final Member member;
    private final AtQuorum atQuorum;
    private final long deadAfterMillis;
    private final AtomicLong firstLivelinessTimestamp;

    public Liveliness(LivelinessStorage livelinessStorage,
        Member member,
        AtQuorum atQuorum,
        long deadAfterMillis,
        AtomicLong firstLivelinessTimestamp) {
        this.livelinessStorage = livelinessStorage;
        this.atQuorum = atQuorum;
        this.member = member;
        this.deadAfterMillis = deadAfterMillis;
        this.firstLivelinessTimestamp = firstLivelinessTimestamp;
    }

    public void blowBubbles(CurrentTimeMillis currentTimeMillis) throws Exception {
        long timestamp = currentTimeMillis.get();
        livelinessStorage.update(setLiveliness -> setLiveliness.set(member, member, timestamp));
        firstLivelinessTimestamp.compareAndSet(-1, timestamp);
    }

    public void acknowledgeOther() throws Exception {
        livelinessStorage.update(setLiveliness -> {
            LivelinessEntry[] otherE = new LivelinessEntry[1];
            boolean[] coldstart = {true};

            //byte[] fromKey = stateKey(versionedPartitionName.getPartitionName(), context, versionedPartitionName.getPartitionVersion(), null, null);
            livelinessStorage.scan(null, null, (rootMember, isSelf, ackMember, timestamp, version) -> {
                if (otherE[0] != null && !otherE[0].rootMember.equals(rootMember)) {
                    if (coldstart[0]) {
                        setLiveliness.set(otherE[0].rootMember, member, otherE[0].timestamp);
                    }
                    otherE[0] = null;
                    coldstart[0] = true;
                }

                if (otherE[0] == null && isSelf && !member.equals(rootMember)) {
                    otherE[0] = new LivelinessEntry(rootMember, timestamp);
                }
                if (otherE[0] != null && member.equals(ackMember) && timestamp != otherE[0].timestamp) {
                    coldstart[0] = false;
                    setLiveliness.set(otherE[0].rootMember, member, otherE[0].timestamp);
                }
                return true;
            });

            if (otherE[0] != null && coldstart[0]) {
                setLiveliness.set(otherE[0].rootMember, member, otherE[0].timestamp);
            }
            return true;
        });
    }

    public long aliveUntilTimestamp() throws Exception {
        if (deadAfterMillis <= 0) {
            return Long.MAX_VALUE;
        }

        long[] currentTimestamp = {-1L};
        long[] latestAck = {-1};
        Set<Member> acked = Sets.newHashSet();
        livelinessStorage.scan(member, null, (rootRingMember, isSelf, ackRingMember, timestamp, version) -> {
            if (currentTimestamp[0] == -1L && isSelf) {
                currentTimestamp[0] = timestamp;
                acked.add(ackRingMember);
            } else if (currentTimestamp[0] != -1L) {
                if (timestamp >= (currentTimestamp[0] - deadAfterMillis)) {
                    latestAck[0] = Math.max(latestAck[0], timestamp);
                    acked.add(ackRingMember);
                }
            }
            return true;
        });

        if (currentTimestamp[0] != -1L && atQuorum.is(acked.size())) {
            return latestAck[0] + deadAfterMillis;
        }
        return -1;
    }

    public long otherAliveUntilTimestamp(Member other) throws Exception {
        if (deadAfterMillis <= 0) {
            return Long.MAX_VALUE;
        }

        long firstTimestamp = firstLivelinessTimestamp.get();
        if (firstTimestamp < 0) {
            return Long.MAX_VALUE;
        }

        long timestamp = livelinessStorage.get(member, other);
        if (timestamp > 0) {
            return timestamp + deadAfterMillis;
        }
        return firstTimestamp + deadAfterMillis;
    }

    private static class LivelinessEntry {

        private final Member rootMember;
        private final long timestamp;

        public LivelinessEntry(Member rootMember,
            long timestamp) {
            this.rootMember = rootMember;
            this.timestamp = timestamp;
        }
    }

}
