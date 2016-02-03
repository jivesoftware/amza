package com.jivesoftware.os.amza.lab.pointers;

import com.jivesoftware.os.lab.api.MergeRawEntry;
import com.jivesoftware.os.lab.io.HeapFiler;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class LABValueMarshaller implements MergeRawEntry {

    @Override
    public byte[] merge(byte[] current, byte[] adding) {
        int currentKeyLength = UIO.bytesInt(current);
        int addingKeyLength = UIO.bytesInt(adding);

        long currentsTimestamp = UIO.bytesLong(current, 4 + currentKeyLength);
        long currentsVersion = UIO.bytesLong(current, 4 + currentKeyLength + 8 + 1);

        long addingsTimestamp = UIO.bytesLong(adding, 4 + addingKeyLength);
        long addingsVersion = UIO.bytesLong(adding, 4 + addingKeyLength + 8 + 1);

        return (currentsTimestamp > addingsTimestamp) || (currentsTimestamp == addingsTimestamp && currentsVersion > addingsVersion) ? current : adding;
    }

    byte[] toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, long pointer) throws IOException {

        HeapFiler indexEntryFiler = new HeapFiler(4 + key.length + 8 + 1 + 8); // TODO somthing better

        UIO.writeByteArray(indexEntryFiler, key, "key", new byte[4]);
        UIO.writeLong(indexEntryFiler, timestamp, "timestamp");
        UIO.writeByte(indexEntryFiler, tombstoned ? (byte) 1 : (byte) 0, "tombstone");
        UIO.writeLong(indexEntryFiler, version, "version");
        UIO.writeLong(indexEntryFiler, pointer, "pointer");
        return indexEntryFiler.getBytes();
    }

}
