package com.jivesoftware.os.amza.lsm.pointers;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.pointers.api.ReadPointerIndex.PointerTx;
import com.jivesoftware.os.lab.api.GetRaw;
import com.jivesoftware.os.lab.api.NextRawEntry;

/**
 *
 * @author jonathan.colt
 */
public class LSMPointerUtils {

    public static <R> R rawToReal(byte[] key, GetRaw rawNextPointer, PointerTx<R> tx) throws Exception {

        return tx.tx((stream) -> rawNextPointer.get(key, (rawEntry, offset, length) -> {

            if (rawEntry == null) {
                return stream.stream(key, -1, false, -1, -1);
            }
            int keyLength = UIO.bytesInt(rawEntry, offset);
            byte[] k = new byte[keyLength];
            System.arraycopy(rawEntry, 4, k, 0, keyLength);
            long timestamp = UIO.bytesLong(rawEntry, offset + 4 + keyLength);
            boolean tombstone = rawEntry[offset + 4 + keyLength + 8] != 0;
            long version = UIO.bytesLong(rawEntry, offset + 4 + keyLength + 8 + 1);
            long walPointerFp = UIO.bytesLong(rawEntry, offset + 4 + keyLength + 8 + 1 + 8);

            return stream.stream(k, timestamp, tombstone, version, walPointerFp);
        }));
    }

    public static <R> R rawToReal(NextRawEntry rawNextPointer, PointerTx<R> tx) throws Exception {

        return tx.tx((stream) -> rawNextPointer.next((rawEntry, offset, length) -> {

            if (rawEntry == null) {
                return stream.stream(null, -1, false, -1, -1);
            }
            int keyLength = UIO.bytesInt(rawEntry, offset);
            byte[] key = new byte[keyLength];
            System.arraycopy(rawEntry, 4, key, 0, keyLength);
            long timestamp = UIO.bytesLong(rawEntry, offset + 4 + keyLength);
            boolean tombstone = rawEntry[offset + 4 + keyLength + 8] != 0;
            long version = UIO.bytesLong(rawEntry, offset + 4 + keyLength + 8 + 1);
            long walPointerFp = UIO.bytesLong(rawEntry, offset + 4 + keyLength + 8 + 1 + 8);

            return stream.stream(key, timestamp, tombstone, version, walPointerFp);
        }));
    }
}
