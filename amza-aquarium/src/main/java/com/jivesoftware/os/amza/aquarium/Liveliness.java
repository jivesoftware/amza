package com.jivesoftware.os.amza.aquarium;

import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class Liveliness {

    private final LivelinessStorage livelinessStorage;
    private final Member member;
    private final AtomicLong firstLivelinessTimestamp;

    public Liveliness(LivelinessStorage livelinessStorage, Member member, AtomicLong firstLivelinessTimestamp) {
        this.livelinessStorage = livelinessStorage;
        this.member = member;
        this.firstLivelinessTimestamp = firstLivelinessTimestamp;
    }

    public void blowBubbles() throws Exception {
        long timestamp = System.currentTimeMillis();
        livelinessStorage.update(setLiveliness -> setLiveliness.set(member, member, timestamp));
        firstLivelinessTimestamp.compareAndSet(-1, timestamp);
    }

    public void acknowledgeOther() throws Exception {
        livelinessStorage.update(setLiveliness -> {
            LivelinessEntry[] otherE = new LivelinessEntry[1];
            boolean[] coldstart = { true };

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
