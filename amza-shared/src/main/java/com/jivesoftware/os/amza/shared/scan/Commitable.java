package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.TxKeyValueStream;

/**
 *
 * @author jonathan.colt
 */
public interface Commitable {

    boolean commitable(Highwaters highwaters, TxKeyValueStream txKeyValueStream) throws Exception;

}
