package com.jivesoftware.os.amza.api.stream;

/**
 * @author jonathan.colt
 */
public interface ClientUpdates {

    boolean updates(CommitKeyValueStream commitKeyValueStream) throws Exception;
}
