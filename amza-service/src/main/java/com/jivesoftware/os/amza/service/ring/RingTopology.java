package com.jivesoftware.os.amza.service.ring;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import java.util.List;

/**
 *
 */
public class RingTopology {

    public final long ringCacheId;
    public final long nodeCacheId;
    public final List<RingMemberAndHost> entries;
    public final int rootMemberIndex;

    public RingTopology(long ringCacheId, long nodeCacheId, List<RingMemberAndHost> entries, int rootMemberIndex) {
        this.ringCacheId = ringCacheId;
        this.nodeCacheId = nodeCacheId;
        this.entries = entries;
        this.rootMemberIndex = rootMemberIndex;
    }

    public int getTakeFromFactor() {
        int ringSize = entries.size();
        return UIO.chunkPower(ringSize < 1 ? 1 : ringSize, 2) - 1;
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
