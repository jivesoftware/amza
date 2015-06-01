package com.jivesoftware.os.amza.shared;

import com.google.common.io.BaseEncoding;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.IOException;
import java.util.Objects;


/**
 *
 * @author jonathan.colt
 */
public class VersionedRegionName implements Comparable<VersionedRegionName> {

    private final RegionName regionName;
    private final long regionVersion;
    private transient int hash = 0;

    public byte[] toBytes() throws IOException {
        HeapFiler memoryFiler = new HeapFiler();
        UIO.writeByte(memoryFiler, 0, "version");
        UIO.writeByteArray(memoryFiler, regionName.toBytes(), "regionName");
        UIO.writeLong(memoryFiler, regionVersion, "regionVersion");
        return memoryFiler.getBytes();
    }

    public static VersionedRegionName fromBytes(byte[] bytes) throws IOException {
        HeapFiler memoryFiler = new HeapFiler(bytes);
        if (UIO.readByte(memoryFiler, "version") == 0) {
            return new VersionedRegionName(
                RegionName.fromBytes(UIO.readByteArray(memoryFiler, "systemRegion")),
                UIO.readLong(memoryFiler, "ringName"));
        }
        throw new IOException("Invalid version:" + bytes[0]);
    }

    public String toBase64() throws IOException {
        return BaseEncoding.base64Url().encode(toBytes());
    }

    public static VersionedRegionName fromBase64(String base64) throws IOException {
        return fromBytes(BaseEncoding.base64Url().decode(base64));
    }

    public VersionedRegionName(RegionName regionName,
        long regionVersion) {
        this.regionName = regionName;
        this.regionVersion = regionVersion;
    }

    public RegionName getRegionName() {
        return regionName;
    }

    public long getRegionVersion() {
        return regionVersion;
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            int h = 3;
            h = 31 * h + Objects.hashCode(this.regionName);
            h = 31 * h + (int) (this.regionVersion ^ (this.regionVersion >>> 32));
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
        final VersionedRegionName other = (VersionedRegionName) obj;
        if (!Objects.equals(this.regionName, other.regionName)) {
            return false;
        }
        if (this.regionVersion != other.regionVersion) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(VersionedRegionName o) {
        int i = regionName.compareTo(o.regionName);
        if (i != 0) {
            return i;
        }
        return Long.compare(regionVersion, o.regionVersion);
    }

    @Override
    public String toString() {
        return "VersionedRegionName{" + "regionName=" + regionName + ", regionVersion=" + regionVersion + '}';
    }


}
