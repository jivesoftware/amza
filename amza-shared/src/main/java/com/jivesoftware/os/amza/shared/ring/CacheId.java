package com.jivesoftware.os.amza.shared.ring;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class CacheId<T> {

    public volatile long currentCacheId = 0;
    public volatile T entry;

    public CacheId(T entry) {
        this.entry = entry;
    }
}
