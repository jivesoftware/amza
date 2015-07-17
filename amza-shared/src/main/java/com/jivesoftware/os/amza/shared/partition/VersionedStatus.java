package com.jivesoftware.os.amza.shared.partition;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.IOException;
import java.util.Objects;

/**
 *
 * @author jonathan.colt
 */
public class VersionedStatus {

    public final TxPartitionStatus.Status status;
    public final long version;
    public final long stripeVersion;

    public byte[] toBytes() throws IOException {
        HeapFiler filer = new HeapFiler();
        UIO.writeByte(filer, 0, "serializationVersion");
        UIO.writeByteArray(filer, status.getSerializedForm(), "status");
        UIO.writeLong(filer, version, "version");
        UIO.writeLong(filer, stripeVersion, "stripeVersion");
        return filer.getBytes();
    }

    public static VersionedStatus fromBytes(byte[] bytes) throws IOException {
        HeapFiler filer = new HeapFiler(bytes);
        byte serializationVersion = UIO.readByte(filer, "serializationVersion");
        if (serializationVersion != 0) {
            throw new IllegalStateException("Failed to deserialize due to an unknown version:" + serializationVersion);
        }
        TxPartitionStatus.Status status = TxPartitionStatus.Status.fromSerializedForm(UIO.readByteArray(filer, "status"));
        long version = UIO.readLong(filer, "version");
        long stripeVersion = UIO.readLong(filer, "stripeVersion");
        return new VersionedStatus(status, version, stripeVersion);
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
