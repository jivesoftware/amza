package com.jivesoftware.os.amza.shared.partition;

import com.google.common.base.Preconditions;
import java.util.Objects;

/**
 *
 * @author jonathan.colt
 */
public class RemoteVersionedStatus {

    public final TxPartitionStatus.Status status;
    public final long version;

    public RemoteVersionedStatus(TxPartitionStatus.Status status, long version) {
        Preconditions.checkNotNull(status, "Status cannot be null");
        this.status = status;
        this.version = version;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 89 * hash + Objects.hashCode(this.status);
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
        final VersionedStatus other = (VersionedStatus) obj;
        if (this.status != other.status) {
            return false;
        }
        if (this.version != other.version) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "RemoteVersionedStatus{" + "status=" + status + ", version=" + version + '}';
    }

}
