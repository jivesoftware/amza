package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface KeyValues {

    boolean consume(KeyValueStream stream) throws Exception;

}
