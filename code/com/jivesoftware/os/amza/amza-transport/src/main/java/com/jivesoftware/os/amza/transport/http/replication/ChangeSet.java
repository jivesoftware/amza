package com.jivesoftware.os.amza.transport.http.replication;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.amza.shared.TableName;
import java.util.List;

public class ChangeSet {

    private final long highestTransactionId;
    private final TableName tableName;
    private final List<String> changes;

    @JsonCreator
    public ChangeSet(
            @JsonProperty("highestTransactionId") long highestTransactionId,
            @JsonProperty("tableName") TableName tableName,
            @JsonProperty("changes") List<String> changes) {
        this.highestTransactionId = highestTransactionId;
        this.tableName = tableName;
        this.changes = changes;
    }

    public long getHighestTransactionId() {
        return highestTransactionId;
    }

    public TableName getTableName() {
        return tableName;
    }

    public List<String> getChanges() {
        return changes;
    }

    @Override
    public String toString() {
        return "ChangeSet{" + "highestTransactionId=" + highestTransactionId + ", tableName=" + tableName + ", changes=" + changes + '}';
    }
}
