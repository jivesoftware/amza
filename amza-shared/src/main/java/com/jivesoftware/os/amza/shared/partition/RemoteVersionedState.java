package com.jivesoftware.os.amza.shared.partition;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.api.partition.PartitionState;
import com.jivesoftware.os.amza.api.partition.VersionedState;
import java.util.Objects;

/**
 *
 * @author jonathan.colt
 */
public class RemoteVersionedState {

    public final PartitionState state;
    public final long version;

    public RemoteVersionedState(PartitionState state, long version) {
        Preconditions.checkNotNull(state, "State cannot be null");
        this.state = state;
        this.version = version;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 89 * hash + Objects.hashCode(this.state);
        hash = 89 * hash + (int) (this.version ^ (this.version >>> 32));
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
        if (this.state != other.state) {
            return false;
        }
        if (this.version != other.version) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "RemoteVersionedState{" + "state=" + state + ", version=" + version + '}';
    }

}
