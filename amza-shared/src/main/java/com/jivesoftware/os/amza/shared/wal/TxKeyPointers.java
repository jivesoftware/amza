package com.jivesoftware.os.amza.shared.wal;

/**
 *
 * @author jonathan.colt
 */
public interface TxKeyPointers {

    boolean consume(TxKeyPointerStream stream) throws Exception;

}
