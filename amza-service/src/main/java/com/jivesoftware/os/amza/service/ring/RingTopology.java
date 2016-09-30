package com.jivesoftware.os.amza.service.ring;

import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.aquarium.Member;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class RingTopology {

    public final long ringCacheId;
    public final long nodeCacheId;
    public final List<RingMemberAndHost> entries;
    public final Set<Member> aquariumMembers;
    public final int rootMemberIndex;

    public RingTopology(long ringCacheId, long nodeCacheId, List<RingMemberAndHost> entries, Set<Member> aquariumMembers, int rootMemberIndex) {
        this.ringCacheId = ringCacheId;
        this.nodeCacheId = nodeCacheId;
        this.entries = entries;
        this.aquariumMembers = aquariumMembers;
        this.rootMemberIndex = rootMemberIndex;
    }

    public int getTakeFromFactor() {
        //TODO We require takeFromFactor to satisfy a quorum, forcing quorum here is a
        //TODO temporary workaround until VersionedRing is pushed down to the partition level.
        /*int ringSize = entries.size();
        return UIO.chunkPower(ringSize < 1 ? 1 : ringSize, 2) - 1;*/
        return Math.max(entries.size() / 2, 1);
    }

    @Override
    public String toString() {
        return "RingTopology{" +
            "ringCacheId=" + ringCacheId +
            ", nodeCacheId=" + nodeCacheId +
            ", entries=" + entries +
            ", rootMemberIndex=" + rootMemberIndex +
            '}';
    }
}
