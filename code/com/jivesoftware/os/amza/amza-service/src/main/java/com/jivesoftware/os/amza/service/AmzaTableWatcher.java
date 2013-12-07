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

import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.TableName;
import java.util.concurrent.ConcurrentHashMap;

public class AmzaTableWatcher implements RowChanges {

    private final RowChanges rowChanges;
    private final ConcurrentHashMap<TableName, RowChanges> watchers = new ConcurrentHashMap<>();

    public AmzaTableWatcher(RowChanges rowChanges) {
        this.rowChanges = rowChanges;
    }

    @Override
    public void changes(RowsChanged changes) throws Exception {
        rowChanges.changes(changes);
        RowChanges watcher = watchers.get(changes.getTableName());
        if (watcher != null) {
            watcher.changes(changes);
        }
    }

    public void watch(TableName tableName, RowChanges rowChanges) throws Exception {
        watchers.put(tableName, rowChanges);
    }

    public RowChanges unwatch(TableName tableName) throws Exception {
        return watchers.remove(tableName);
    }
}
