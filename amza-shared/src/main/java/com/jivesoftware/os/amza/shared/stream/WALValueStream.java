package com.jivesoftware.os.amza.shared.stream;

/**
 *
 * @author jonathan.colt
 */
public interface WALValueStream {

    boolean stream(byte[] value,
        long valueTimestamp,
        boolean valueTombstone,
        long valueVersion) throws Exception;

}
