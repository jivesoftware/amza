package com.jivesoftware.os.amza.api.partition;

import com.jivesoftware.os.amza.api.filer.UIO;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class StorageVersion {

    private static final byte VERSION = (byte) 0;

    public final long partitionVersion;
    public final long stripeVersion;

    public byte[] toBytes() throws IOException {
        byte[] asBytes = new byte[1 + 8 + 8];
        asBytes[0] = VERSION; // version
        UIO.longBytes(partitionVersion, asBytes, 1);
        UIO.longBytes(stripeVersion, asBytes, 1 + 8);
        return asBytes;
    }

    public static StorageVersion fromBytes(byte[] bytes) throws IOException {
        if (bytes[0] == VERSION) {
            long ringVersion = UIO.bytesLong(bytes, 1);
            long stripeVersion = UIO.bytesLong(bytes, 1 + 8);
            return new StorageVersion(ringVersion, stripeVersion);
        }
        throw new IllegalStateException("Failed to deserialize due to an unknown version:" + bytes[0]);
    }

    public StorageVersion(long partitionVersion, long stripeVersion) {
        this.partitionVersion = partitionVersion;
        this.stripeVersion = stripeVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StorageVersion that = (StorageVersion) o;

        if (partitionVersion != that.partitionVersion) {
            return false;
        }
        return stripeVersion == that.stripeVersion;
    }

    @Override
    public int hashCode() {
        int result = (int) (partitionVersion ^ (partitionVersion >>> 32));
        result = 31 * result + (int) (stripeVersion ^ (stripeVersion >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "StorageVersion{" + "partitionVersion=" + partitionVersion + ", stripeVersion=" + stripeVersion + '}';
    }

}
