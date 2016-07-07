package com.jivesoftware.os.amza.api.stream;

/**
 *
 * @author jonathan.colt
 */
public interface KeyValuePointerStream {

    boolean stream(byte[] prefix,
        byte[] key,
        byte[] value,
        long valueTimestamp,
        boolean valueTombstoned,
        long valueVersion,
        long pointerTimestamp,
        boolean pointerTombstoned,
        long pointerVersion,
        long pointerFp) throws Exception;

}
