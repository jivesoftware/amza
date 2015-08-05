package com.jivesoftware.os.amza.shared.stream;

/**
 *
 * @author jonathan.colt
 */
public interface KeyValueStream {

    boolean stream(byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) throws Exception;

}
