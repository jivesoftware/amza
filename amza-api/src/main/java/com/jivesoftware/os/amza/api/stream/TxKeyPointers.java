package com.jivesoftware.os.amza.api.stream;

/**
 *
 * @author jonathan.colt
 */
public interface TxKeyPointers {

    boolean consume(TxKeyPointerStream stream) throws Exception;

}
