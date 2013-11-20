/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
