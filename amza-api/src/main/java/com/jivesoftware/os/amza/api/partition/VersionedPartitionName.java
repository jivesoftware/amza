package com.jivesoftware.os.amza.api.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.io.BaseEncoding;
import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.api.filer.UIO;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;


/**
 * @author jonathan.colt
 */
public class VersionedPartitionName implements Comparable<VersionedPartitionName>, Serializable {

    public static final long STATIC_VERSION = 0;

    private final PartitionName partitionName;
    private final long partitionVersion;
    private transient int hash = 0;

    public byte[] toBytes() throws IOException {
        byte[] partitionNameBytes = partitionName.toBytes();

        byte[] asBytes = new byte[1 + 4 + partitionNameBytes.length + 8];
        asBytes[0] = 0; // version
        UIO.intBytes(partitionNameBytes.length, asBytes, 1);
        System.arraycopy(partitionNameBytes, 0, asBytes, 1 + 4, partitionNameBytes.length);
        UIO.longBytes(partitionVersion, asBytes, 1 + 4 + partitionNameBytes.length);
        return asBytes;
    }

    public int sizeInBytes() {
        return 1 + 4 + partitionName.sizeInBytes() + 8;
    }

    @JsonCreator
    public VersionedPartitionName(@JsonProperty("partitionName") PartitionName partitionName,
        @JsonProperty("partitionVersion") long partitionVersion) {
        this.partitionName = partitionName;
        this.partitionVersion = partitionVersion;
    }

    public String toBase64() throws IOException {
        return BaseEncoding.base64Url().encode(toBytes());
    }

    public static VersionedPartitionName fromBase64(String base64, AmzaInterner interner) throws Exception {
        return interner.internVersionedPartitionNameBase64(base64);
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
        return this.partitionVersion == other.partitionVersion;
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
