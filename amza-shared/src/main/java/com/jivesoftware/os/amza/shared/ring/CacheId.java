package com.jivesoftware.os.amza.shared.ring;

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
