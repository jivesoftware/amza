package com.jivesoftware.os.amza.lsm;

/**
 *
 * @author jonathan.colt
 */
public class Dumping {
    /*
     readable.read(entryBuffer, 0, entryLength);
                        int keyLength = UIO.bytesInt(entryBuffer, 0);
                        byte[] key = new byte[keyLength];
                        System.arraycopy(entryBuffer, 4, key, 0, keyLength);
                        long timestamp = UIO.bytesLong(entryBuffer, 4 + keyLength);
                        boolean tombstone = entryBuffer[4 + keyLength + 8] != 0;
                        long version = UIO.bytesLong(entryBuffer, 4 + keyLength + 8 + 1);
                        long walPointerFp = UIO.bytesLong(entryBuffer, 4 + keyLength + 8 + 1 + 8);
                        return stream.stream(
                            key,
                            timestamp,
                            tombstone,
                            version,
                            walPointerFp);
    */
}
