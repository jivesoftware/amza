package com.jivesoftware.os.amza.storage;

import com.jivesoftware.os.amza.shared.TableName;

public interface RowMarshallerProvider {

    <K, V, R> RowMarshaller<K, V, R> getRowMarshaller(TableName<K, V> tableName) throws Exception;
}