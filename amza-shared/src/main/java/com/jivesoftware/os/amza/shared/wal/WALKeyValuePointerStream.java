package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface WALKeyValuePointerStream {

    boolean stream(byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned,
        long pointerTimestamp, boolean pointerTombstoned, long pointerFp) throws Exception;

}
