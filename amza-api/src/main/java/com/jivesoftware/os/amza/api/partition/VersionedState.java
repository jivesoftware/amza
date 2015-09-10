package com.jivesoftware.os.amza.api.partition;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.aquarium.Waterline;
import java.util.Objects;

/**
 * @author jonathan.colt
 */
public class VersionedState {

    public final Waterline waterline;
    public final boolean isOnline;
    public final StorageVersion storageVersion;

    public VersionedState(Waterline waterline, boolean isOnline, StorageVersion storageVersion) {
        Preconditions.checkNotNull(waterline, "Waterline cannot be null");
        Preconditions.checkNotNull(storageVersion, "StorageVersion cannot be null");
        this.waterline = waterline;
        this.isOnline = isOnline;
        this.storageVersion = storageVersion;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + Objects.hashCode(this.waterline);
        hash = 29 * hash + (this.isOnline ? 1 : 0);
        hash = 29 * hash + Objects.hashCode(this.storageVersion);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final VersionedState other = (VersionedState) obj;
        if (this.waterline != other.waterline) {
            return false;
        }
        if (this.isOnline != other.isOnline) {
            return false;
        }
        if (!Objects.equals(this.storageVersion, other.storageVersion)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "VersionedState{" + "waterline=" + waterline + ", isOnline=" + isOnline + ", storageVersion=" + storageVersion + '}';
    }

}
