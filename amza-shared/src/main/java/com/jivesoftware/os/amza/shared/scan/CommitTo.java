package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.shared.wal.WALValue;

/**
 *
 * @author jonathan.colt
 */
public interface CommitTo {

    RowsChanged commit(Commitable<WALValue> commitable) throws Exception;
}
