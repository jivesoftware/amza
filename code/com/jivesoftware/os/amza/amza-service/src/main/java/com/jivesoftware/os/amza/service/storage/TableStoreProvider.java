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
package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsStorage;
import com.jivesoftware.os.amza.shared.RowsStorageProvider;
import com.jivesoftware.os.amza.shared.TableName;
import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TableStoreProvider {

    private final File workingDirectory;
    private final String storeName;
    private final RowsStorageProvider  tableStorageProvider;
    private final RowChanges tableStateChanges;
    private final ConcurrentHashMap<TableName, TableStore> rowsStores = new ConcurrentHashMap<>();

    public TableStoreProvider(File workingDirectory,
            String storeName,
            RowsStorageProvider tableStorageProvider,
            RowChanges tableStateChanges) {
        this.workingDirectory = workingDirectory;
        this.storeName = storeName;
        this.tableStorageProvider = tableStorageProvider;
        this.tableStateChanges = tableStateChanges;
    }

    public String getName() {
        return storeName;
    }

    synchronized public TableStore getRowsStore(TableName tableName) throws Exception {
        TableStore tableStore = rowsStores.get(tableName);
        if (tableStore == null) {
            RowsStorage rowsStorage = tableStorageProvider.createRowsStorage(workingDirectory, storeName, tableName);
            tableStore = new TableStore(rowsStorage, tableStateChanges);
            tableStore.load();
            rowsStores.put(tableName, tableStore);
        }
        return tableStore;
    }


    public Set<Map.Entry<TableName, TableStore>> getTableStores() {
        return rowsStores.entrySet();
    }
}