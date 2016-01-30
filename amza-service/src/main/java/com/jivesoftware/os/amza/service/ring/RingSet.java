package com.jivesoftware.os.amza.service.ring;

import com.jivesoftware.os.amza.api.value.ConcurrentBAHash;

/**
 *
 */
public class RingSet {

    public final long memberCacheId;
    public final ConcurrentBAHash<byte[]> ringNames;

    public RingSet(long memberCacheId, ConcurrentBAHash<byte[]> ringNames) {
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
