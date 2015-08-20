package com.jivesoftware.os.amza.api.partition;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.api.filer.UIO;
import java.io.IOException;
import java.util.Objects;

/**
 * @author jonathan.colt
 */
public class VersionedStatus {

    public final TxPartitionStatus.Status status;
    public final long version;
    public final long stripeVersion;

    public byte[] toBytes() throws IOException {
        byte[] asBytes = new byte[1 + 1 + 8 + 8];
        asBytes[0] = 0; // version
        asBytes[1] = status.getSerializedForm();
        UIO.longBytes(version, asBytes, 1 + 1);
        UIO.longBytes(stripeVersion, asBytes, 1 + 1 + 8);
        return asBytes;
    }

    public static VersionedStatus fromBytes(byte[] bytes) throws IOException {
        if (bytes[0] == 0) {
            TxPartitionStatus.Status status = TxPartitionStatus.Status.fromSerializedForm(bytes[1]);
            long version = UIO.bytesLong(bytes, 1 + 1);
            long stripeVersion = UIO.bytesLong(bytes, 1 + 1 + 8);
            return new VersionedStatus(status, version, stripeVersion);
        }
        throw new IllegalStateException("Failed to deserialize due to an unknown version:" + bytes[0]);
    }

    public VersionedStatus(TxPartitionStatus.Status status, long version, long stripeVersion) {
        Preconditions.checkNotNull(status, "Status cannot be null");
        this.status = status;
        this.version = version;
        this.stripeVersion = stripeVersion;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 53 * hash + Objects.hashCode(this.status);
        hash = 53 * hash + (int) (this.version ^ (this.version >>> 32));
        hash = 53 * hash + (int) (this.stripeVersion ^ (this.stripeVersion >>> 32));
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
        if (this.stripeVersion != other.stripeVersion) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "VersionedStatus{" + "status=" + status + ", version=" + version + ", stripeVersion=" + stripeVersion + '}';
    }

}
