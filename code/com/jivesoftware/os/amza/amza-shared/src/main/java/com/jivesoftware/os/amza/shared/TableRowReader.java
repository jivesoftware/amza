package com.jivesoftware.os.amza.shared;

public interface TableRowReader<R> {

    static interface Stream<S> {

        boolean stream(S row) throws Exception;
    }

    void read(boolean reverse, Stream<R> rowStream) throws Exception;
}