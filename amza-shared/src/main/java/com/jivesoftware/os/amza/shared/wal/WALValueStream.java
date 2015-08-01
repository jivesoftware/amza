package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface WALValueStream {

    boolean stream(byte[] value,
        long valueTimestamp,
        boolean valueTombstone) throws Exception;

}
