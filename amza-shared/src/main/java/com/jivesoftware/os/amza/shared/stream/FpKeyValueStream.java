package com.jivesoftware.os.amza.shared.stream;

/**
 *
 * @author jonathan.colt
 */
public interface FpKeyValueStream {

    boolean stream(long fp, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion) throws Exception;

}
