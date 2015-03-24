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

import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageProvider;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class WALs {

    private final File workingDirectory;
    private final String storeName;
    private final WALStorageProvider walStorageProvider;
    private final ConcurrentHashMap<RegionName, WALStorage> walStores = new ConcurrentHashMap<>();
    private final StripingLocksProvider locksProvider = new StripingLocksProvider(1024); // TODO expose to config

    public WALs(File workingDirectory,
        String storeName,
        WALStorageProvider rowStorageProvider) {
        this.workingDirectory = workingDirectory;
        this.storeName = storeName;
        this.walStorageProvider = rowStorageProvider;
    }

    public String getName() {
        return storeName;
    }

    public WALStorage get(RegionName regionName) throws Exception {
        synchronized (locksProvider.lock(regionName, 1234)) {
            WALStorage storage = walStores.get(regionName);
            if (storage == null) {
                storage = walStorageProvider.create(workingDirectory, storeName, regionName, null);
                storage.load();
                walStores.put(regionName, storage);
            }
            return storage;
        }
    }

    public Set<Map.Entry<RegionName, WALStorage>> getAll() {
        return walStores.entrySet();
    }
}
