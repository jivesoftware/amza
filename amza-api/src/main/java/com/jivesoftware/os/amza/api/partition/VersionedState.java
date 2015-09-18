package com.jivesoftware.os.amza.api.partition;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;

/**
 * @author jonathan.colt
 */
public class VersionedState {

    private final LivelyEndState livelyEndState;
    private final StorageVersion storageVersion;

    public VersionedState(LivelyEndState livelyEndState, StorageVersion storageVersion) {
        Preconditions.checkNotNull(livelyEndState, "LivelyEndState cannot be null");
        Preconditions.checkNotNull(storageVersion, "StorageVersion cannot be null");
        this.livelyEndState = livelyEndState;
        this.storageVersion = storageVersion;
    }

    public LivelyEndState getLivelyEndState() {
        return livelyEndState;
    }

    public StorageVersion getStorageVersion() {
        return storageVersion;
    }

    public boolean isCurrentState(State state) {
        return livelyEndState.getCurrentState() == state;
    }

    public long getPartitionVersion() {
        return storageVersion.partitionVersion;
    }

    @Override
    public boolean equals(Object obj) {
        throw new UnsupportedOperationException("Stop that");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Stop that");
    }

    @Override
    public String toString() {
        return "VersionedState{" + "livelyEndState=" + livelyEndState + ", storageVersion=" + storageVersion + '}';
    }

}
