package com.jivesoftware.os.amza.api.partition;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.aquarium.State;

/**
 * @author jonathan.colt
 */
public class VersionedState {

    public final State state;
    public final StorageVersion storageVersion;

    public VersionedState(State state, StorageVersion storageVersion) {
        Preconditions.checkNotNull(state, "State cannot be null");
        Preconditions.checkNotNull(storageVersion, "StorageVersion cannot be null");
        this.state = state;
        this.storageVersion = storageVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        VersionedState that = (VersionedState) o;

        if (state != that.state) {
            return false;
        }
        return !(storageVersion != null ? !storageVersion.equals(that.storageVersion) : that.storageVersion != null);

    }

    @Override
    public int hashCode() {
        int result = state != null ? state.hashCode() : 0;
        result = 31 * result + (storageVersion != null ? storageVersion.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "VersionedState{" +
            "state=" + state +
            ", storageVersion=" + storageVersion +
            '}';
    }
}
