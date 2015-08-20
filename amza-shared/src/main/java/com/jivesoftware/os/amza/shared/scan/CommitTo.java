package com.jivesoftware.os.amza.shared.scan;

import com.jivesoftware.os.amza.api.scan.Commitable;

/**
 *
 * @author jonathan.colt
 */
public interface CommitTo {

    RowsChanged commit(byte[] prefix, Commitable commitable) throws Exception;
}
