package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.shared.TableName;
import java.util.Map;

/**
 *
 */
public class ChangeSet {
    private final TableName tableName;
    private final Map table;

    public ChangeSet(TableName tableName, Map table) {
        this.tableName = tableName;
        this.table = table;
    }

    public TableName getTableName() {
        return tableName;
    }

    public Map getTable() {
        return table;
    }
    
}
