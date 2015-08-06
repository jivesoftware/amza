package com.jivesoftware.os.amza.shared.stream;

/**
 *
 * @author jonathan.colt
 */
public interface TxKeyPointers {

    boolean consume(TxKeyPointerStream stream) throws Exception;

}
