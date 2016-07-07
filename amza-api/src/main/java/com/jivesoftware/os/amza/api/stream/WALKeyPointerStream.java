package com.jivesoftware.os.amza.api.stream;

/**
 *
 * @author jonathan.colt
 */
public interface WALKeyPointerStream {

    boolean stream(byte[] prefix,
        byte[] key,
        long timestamp,
        boolean tombstoned,
        long version,
        long fp,
        boolean hasValue,
        byte[] value) throws Exception;

}
