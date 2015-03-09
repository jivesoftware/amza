package com.jivesoftware.os.amza.shared;

/**
 *
 * @author jonathan.colt
 */
public interface RowsTx<T> {

    public static interface RowsWrite<T, R> {

        R write(RowWriter<T> writer) throws Exception;
    }

    public static interface RowsRead<T, R> {

        R read(RowReader<T> reader) throws Exception;
    }

    <R> R write(RowsWrite<T, R> write) throws Exception;

    <R> R read(RowsRead<T, R> read) throws Exception;

    void compact(RowsIndex rowsIndex) throws Exception;
}
