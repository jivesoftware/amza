package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface KeyValues {

    void consume(KeyValueStream stream) throws Exception;

}
