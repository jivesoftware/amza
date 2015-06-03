package com.jivesoftware.os.amza.shared.partition;

import com.google.common.io.BaseEncoding;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.IOException;
import java.util.Objects;


/**
 *
 * @author jonathan.colt
 */
public class VersionedPartitionName implements Comparable<VersionedPartitionName> {

    private final PartitionName partitionName;
    private final long partitionVersion;
    private transient int hash = 0;

    public byte[] toBytes() throws IOException {
        HeapFiler memoryFiler = new HeapFiler();
        UIO.writeByte(memoryFiler, 0, "version");
        UIO.writeByteArray(memoryFiler, partitionName.toBytes(), "partitionName");
        UIO.writeLong(memoryFiler, partitionVersion, "partitionVersion");
        return memoryFiler.getBytes();
    }

    public static VersionedPartitionName fromBytes(byte[] bytes) throws IOException {
        HeapFiler memoryFiler = new HeapFiler(bytes);
        if (UIO.readByte(memoryFiler, "version") == 0) {
            return new VersionedPartitionName(
                PartitionName.fromBytes(UIO.readByteArray(memoryFiler, "systemPartition")),
                UIO.readLong(memoryFiler, "ringName"));
        }
        throw new IOException("Invalid version:" + bytes[0]);
    }

    public String toBase64() throws IOException {
        return BaseEncoding.base64Url().encode(toBytes());
    }

    public static VersionedPartitionName fromBase64(String base64) throws IOException {
        return fromBytes(BaseEncoding.base64Url().decode(base64));
    }

    public VersionedPartitionName(PartitionName partitionName,
        long partitionVersion) {
        this.partitionName = partitionName;
        this.partitionVersion = partitionVersion;
    }

    public PartitionName getPartitionName() {
        return partitionName;
    }

    public long getPartitionVersion() {
        return partitionVersion;
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            int h = 3;
            h = 31 * h + Objects.hashCode(this.partitionName);
            h = 31 * h + (int) (this.partitionVersion ^ (this.partitionVersion >>> 32));
            this.hash = h;
        }
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
        final VersionedPartitionName other = (VersionedPartitionName) obj;
        if (!Objects.equals(this.partitionName, other.partitionName)) {
            return false;
        }
        if (this.partitionVersion != other.partitionVersion) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(VersionedPartitionName o) {
        int i = partitionName.compareTo(o.partitionName);
        if (i != 0) {
            return i;
        }
        return Long.compare(partitionVersion, o.partitionVersion);
    }

    @Override
    public String toString() {
        return "VersionedPartitionName{" + "partitionName=" + partitionName + ", partitionVersion=" + partitionVersion + '}';
    }


}
