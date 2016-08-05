package com.jivesoftware.os.amza.api.stream;

/**
 *
 * @author jonathan.colt
 */
public interface KeyStream {

    boolean stream(byte[] prefix,
        byte[] key,
        long valueTimestamp,
        boolean valueTombstoned,
        long valueVersion) throws Exception;

}
