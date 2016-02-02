package com.jivesoftware.os.amza.lsm.pointers;

import com.jivesoftware.os.lab.api.MergeRawEntry;
import com.jivesoftware.os.lab.io.HeapFiler;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class LSMValueMarshaller implements MergeRawEntry {

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

//    public static NextPointer rawToReal(NextRawEntry rawNextPointer) throws Exception {
//
//        return (stream) -> rawNextPointer.next((rawEntry, offset, length) -> {
//            int keyLength = UIO.bytesInt(rawEntry, offset);
//            byte[] key = new byte[keyLength];
//            System.arraycopy(rawEntry, 4, key, 0, keyLength);
//            long timestamp = UIO.bytesLong(rawEntry, offset + 4 + keyLength);
//            boolean tombstone = rawEntry[offset + 4 + keyLength + 8] != 0;
//            long version = UIO.bytesLong(rawEntry, offset + 4 + keyLength + 8 + 1);
//            long walPointerFp = UIO.bytesLong(rawEntry, offset + 4 + keyLength + 8 + 1 + 8);
//
//            return stream.stream(key, timestamp, tombstone, version, walPointerFp);
//        });
//    }
//
//    public static NextRawEntry realToRaw(NextPointer nextPointer) throws Exception {
//
//        HeapFiler indexEntryFiler = new HeapFiler(1024); // TODO somthing better
//        byte[] lengthBuffer = new byte[4];
//
//        return (stream) -> nextPointer.next((byte[] key, long timestamp, boolean tombstoned, long version, long pointer) -> {
//            indexEntryFiler.reset();
//
//            UIO.writeByteArray(indexEntryFiler, key, "key", lengthBuffer);
//            UIO.writeLong(indexEntryFiler, timestamp, "timestamp");
//            UIO.writeByte(indexEntryFiler, tombstoned ? (byte) 1 : (byte) 0, "tombstone");
//            UIO.writeLong(indexEntryFiler, version, "version");
//            UIO.writeLong(indexEntryFiler, pointer, "pointer");
//
//            byte[] rawEntry = indexEntryFiler.copyUsedBytes();
//            return stream.stream(rawEntry, 0, rawEntry.length);
//        });
//
//    }

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
