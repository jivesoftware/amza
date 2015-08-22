package com.jivesoftware.os.amza.shared.stream;

/**
 *
 * @author jonathan.colt
 */
public interface WALMergeKeyPointerStream {

    static byte added = 1;
    static byte clobbered = 2;
    static byte ignored = 3;

    boolean stream(byte mode,
        byte[] prefix,
        byte[] key,
        long timestamp,
        boolean tombstoned,
        long version,
        long fp) throws Exception;

}
