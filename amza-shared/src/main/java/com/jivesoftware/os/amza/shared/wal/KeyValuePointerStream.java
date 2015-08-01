package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface KeyValuePointerStream {

    boolean stream(byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned,
        long pointerTimestamp, boolean pointerTombstoned, long pointerFp) throws Exception;

}
