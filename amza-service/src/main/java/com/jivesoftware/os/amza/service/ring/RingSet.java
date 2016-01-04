package com.jivesoftware.os.amza.service.ring;

import com.jivesoftware.os.filer.io.IBA;
import java.util.Set;

/**
 *
 */
public class RingSet {

    public final long memberCacheId;
    public final Set<IBA> ringNames;

    public RingSet(long memberCacheId, Set<IBA> ringNames) {
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
