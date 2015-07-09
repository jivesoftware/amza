package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.take.Highwaters;

/**
 *
 * @author jonathan.colt
 */
public interface Commitable {

    boolean commitable(Highwaters highwaters, TxKeyValueStream txKeyValueStream) throws Exception;

}
