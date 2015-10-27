package com.jivesoftware.os.amza.shared.ring;

import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
