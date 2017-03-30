package com.jivesoftware.os.amza.api;

import com.google.common.io.BaseEncoding;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.jive.utils.collections.bah.ConcurrentBAHash;

/**
 *
 */
public class AmzaInterner {

    private final BAInterner baInterner = new BAInterner();
    private final ConcurrentBAHash<RingMember> ringMemberInterner = new ConcurrentBAHash<>(3, true, 1024);

    public long size() {
        return ringMemberInterner.size() + baInterner.size();
    }

    public RingMember internRingMember(byte[] bytes, int offset, int length) throws InterruptedException {
        if (bytes == null || length == -1) {
            return null;
        }

        if (bytes[offset] == 0) {
            int o = offset + 1;
            int l = length - 1;
            RingMember ringMember = ringMemberInterner.get(bytes, o, l);
            if (ringMember == null) {
                byte[] key = new byte[l];
                System.arraycopy(bytes, o, key, 0, l);
                ringMember = new RingMember(key);
                ringMemberInterner.put(key, ringMember);
            }
            return ringMember;
        }
        return null;
    }

    public PartitionName internPartitionName(byte[] bytes, int offset, int length) throws InterruptedException {
        if (bytes == null || length == -1) {
            return null;
        }

        int o = offset;
        if (bytes[o] == 0) { // version
            o++;
            boolean systemPartition = (bytes[o] == 1);
            o++;
            int ringNameLength = UIO.bytesInt(bytes, o);
            o += 4;
            byte[] ringName = baInterner.intern(bytes, o, ringNameLength);
            o += ringNameLength;
            int nameLength = UIO.bytesInt(bytes, o);
            o += 4;
            byte[] name = baInterner.intern(bytes, o, nameLength);
            return new PartitionName(systemPartition, ringName, name);
        }
        throw new RuntimeException("Invalid version:" + bytes[0]);
    }

    public PartitionName internPartitionNameBase64(String base64) throws InterruptedException {
        byte[] bytes = BaseEncoding.base64Url().decode(base64);
        return internPartitionName(bytes, 0, bytes.length);
    }

    public VersionedPartitionName internVersionedPartitionName(byte[] bytes, int offset, int length) throws InterruptedException {
        if (bytes == null || length == -1) {
            return null;
        }

        int o = offset;
        if (bytes[o] == 0) { // version
            o++;
            int partitionNameBytesLength = UIO.bytesInt(bytes, o);
            o += 4;
            PartitionName partitionName = internPartitionName(bytes, o, partitionNameBytesLength);
            o += partitionNameBytesLength;
            long version = UIO.bytesLong(bytes, o);
            return new VersionedPartitionName(partitionName, version);
        }
        throw new RuntimeException("Invalid version:" + bytes[0]);
    }

    public VersionedPartitionName internVersionedPartitionNameBase64(String base64) throws InterruptedException {
        byte[] bytes = BaseEncoding.base64Url().decode(base64);
        return internVersionedPartitionName(bytes, 0, bytes.length);
    }

    public byte[] internRingName(byte[] key, int offset, int length) throws InterruptedException {
        return baInterner.intern(key, offset, length);
    }
}
