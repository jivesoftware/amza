package com.jivesoftware.os.amza.shared;

/**
 *
 * @author jonathan.colt
 */
public interface Commitable<S> {

    void commitable(Highwaters highwaters, Scan<S> scan) throws Exception;

}
