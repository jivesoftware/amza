package com.jivesoftware.os.amza.shared;

import com.google.common.base.Optional;

/**
 *
 * @author jonathan.colt
 * @param <T>
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

    RowsIndex load(TableName tableName) throws Exception;

    Optional<Compacted> compact(TableName tableName, long removeTombstonedOlderThanNMillis, RowsIndex rowIndex) throws Exception;

    public interface Compacted {

        RowsIndex getCompactedRowsIndex() throws Exception;
    }
}
