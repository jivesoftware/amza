package com.jivesoftware.os.amza.service.ring;

import com.jivesoftware.os.jive.utils.collections.bah.ConcurrentBAHash;

/**
 *
 */
public class RingSet {

    public final long memberCacheId;
    public final ConcurrentBAHash<Integer> ringNames;

    public RingSet(long memberCacheId, ConcurrentBAHash<Integer> ringNames) {
        this.memberCacheId = memberCacheId;
        this.ringNames = ringNames;
    }

    @Override
    public String toString() {
        return "RingSet{"
            + "memberCacheId=" + memberCacheId
            + ", ringNames=" + ringNames
            + '}';
    }
}
