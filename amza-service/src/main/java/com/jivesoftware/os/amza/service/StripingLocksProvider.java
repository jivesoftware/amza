package com.jivesoftware.os.amza.service;

/**
 *
 * @author jonathan.colt
 */
public class StripingLocksProvider<K>  {

    private final StripingLock[] locks;

    public StripingLocksProvider(int numLocks) {
        locks = new StripingLock[numLocks];
        for (int i = 0; i < numLocks; i++) {
            locks[i] = new StripingLock();
        }
    }

    public Object lock(K toLock, int seed) {
        return locks[Math.abs((toLock.hashCode() ^ seed) % locks.length)];
    }

    static private class StripingLock {
    }
}