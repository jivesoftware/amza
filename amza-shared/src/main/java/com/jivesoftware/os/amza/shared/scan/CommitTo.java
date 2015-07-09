package com.jivesoftware.os.amza.shared.scan;

/**
 *
 * @author jonathan.colt
 */
public interface CommitTo {

    RowsChanged commit(Commitable commitable) throws Exception;
}
