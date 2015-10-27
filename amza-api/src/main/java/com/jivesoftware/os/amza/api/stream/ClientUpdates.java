package com.jivesoftware.os.amza.api.stream;

/**
 * @author jonathan.colt
 */
public interface ClientUpdates {

    boolean updates(UnprefixedTxKeyValueStream txKeyValueStream) throws Exception;
}
