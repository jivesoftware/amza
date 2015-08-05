package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.stream.UnprefixedTxKeyValueStream;

/**
 *
 * @author jonathan.colt
 */
public interface Commitable {

    boolean commitable(Highwaters highwaters, UnprefixedTxKeyValueStream txKeyValueStream) throws Exception;

}
