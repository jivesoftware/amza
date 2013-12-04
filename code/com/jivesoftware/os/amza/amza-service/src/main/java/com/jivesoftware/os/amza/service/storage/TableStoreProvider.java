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

import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStateChanges;
import com.jivesoftware.os.amza.shared.TableStorage;
import com.jivesoftware.os.amza.shared.TableStorageProvider;
import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TableStoreProvider {

    private final File workingDirectory;
    private final String storeName;
    private final TableStorageProvider  tableStorageProvider;
    private final TableStateChanges tableStateChanges;
    private final ConcurrentHashMap<TableName, TableStore> tableStores = new ConcurrentHashMap<>();

    public TableStoreProvider(File workingDirectory,
            String storeName,
            TableStorageProvider tableStorageProvider,
            TableStateChanges tableStateChanges) {
        this.workingDirectory = workingDirectory;
        this.storeName = storeName;
        this.tableStorageProvider = tableStorageProvider;
        this.tableStateChanges = tableStateChanges;
    }

    public String getName() {
        return storeName;
    }

    synchronized public TableStore getTableStore(TableName tableName) throws Exception {
        TableStore tableStore = tableStores.get(tableName);
        if (tableStore == null) {
            TableStorage tableStorage = tableStorageProvider.createTableStorage(workingDirectory, storeName, tableName);
            ReadWriteTableStore readWriteTableStore = new ReadWriteTableStore(tableStorage, tableStateChanges);
            readWriteTableStore.load();
            tableStore = new TableStore(readWriteTableStore);
            tableStores.put(tableName, tableStore);
        }
        return tableStore;
    }


    public Set<Map.Entry<TableName, TableStore>> getTableStores() {
        return tableStores.entrySet();
    }
}