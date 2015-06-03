package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.take.Highwaters;

/**
 *
 * @author jonathan.colt
 */
public interface Commitable<S> {

    void commitable(Highwaters highwaters, Scan<S> scan) throws Exception;

}
