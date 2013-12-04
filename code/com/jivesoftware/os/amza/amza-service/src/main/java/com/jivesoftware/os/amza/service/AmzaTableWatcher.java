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
package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStateChanges;
import java.util.concurrent.ConcurrentHashMap;

public class AmzaTableWatcher implements TableStateChanges {

    private final TableStateChanges tableStateChanges;
    private final ConcurrentHashMap<TableName, TableStateChanges> watchers = new ConcurrentHashMap<>();

    public AmzaTableWatcher(TableStateChanges tableStateChanges) {
        this.tableStateChanges = tableStateChanges;
    }

    @Override
    public void changes(TableName tableName, TableDelta changes) throws Exception {
        tableStateChanges.changes(tableName, changes);
        TableStateChanges watcher = watchers.get(tableName);
        if (watcher != null) {
            watcher.changes(tableName, changes);
        }
    }

    public void watch(TableName tableName, TableStateChanges tableStateChanges) throws Exception {
        watchers.put(tableName, tableStateChanges);
    }

    public TableStateChanges unwatch(TableName tableName) throws Exception {
        return watchers.remove(tableName);
    }
}
