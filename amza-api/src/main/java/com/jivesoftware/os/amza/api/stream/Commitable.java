package com.jivesoftware.os.amza.api.stream;

import com.jivesoftware.os.amza.api.take.Highwaters;

/**
 *
 * @author jonathan.colt
 */
public interface Commitable {

    boolean commitable(Highwaters highwaters, UnprefixedTxKeyValueStream txKeyValueStream) throws Exception;

}
