package com.jivesoftware.os.amza.shared;

/**
 *
 * @author jonathan.colt
 */
public interface Highwaters {

    void highwater(WALHighwater highwater) throws Exception;
}
