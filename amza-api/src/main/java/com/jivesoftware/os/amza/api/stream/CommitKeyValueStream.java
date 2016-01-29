package com.jivesoftware.os.amza.api.stream;

public interface CommitKeyValueStream {

    boolean commit(byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) throws Exception;

}
