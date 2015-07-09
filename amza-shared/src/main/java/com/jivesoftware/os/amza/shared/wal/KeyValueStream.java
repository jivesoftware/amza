package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface KeyValueStream {

    boolean stream(byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) throws Exception;

}
