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
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageProvider;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RegionProvider {

    private final String[] workingDirectories;
    private final String domain;
    private final WALStorageProvider walStorageProvider;
    private final RowChanges rwoChanges;
    private final WALReplicator walReplicator;
    private final ConcurrentHashMap<RegionName, RegionStore> regionStores = new ConcurrentHashMap<>();
    private final StripingLocksProvider locksProvider = new StripingLocksProvider(1024); // TODO expose to config

    public RegionProvider(String[] workingDirectories,
        String domain,
        WALStorageProvider walStorageProvider,
        RowChanges rowChanges,
        WALReplicator rowReplicator) {
        this.workingDirectories = workingDirectories;
        this.domain = domain;
        this.walStorageProvider = walStorageProvider;
        this.rwoChanges = rowChanges;
        this.walReplicator = rowReplicator;
    }

    public String getName() {
        return domain;
    }

    public RegionStore get(RegionName regionName) throws Exception {
        synchronized (locksProvider.lock(regionName, 1234)) {
            RegionStore regionStore = regionStores.get(regionName);
            if (regionStore == null) {
                File workingDirectory = new File(workingDirectories[Math.abs(regionName.hashCode()) % workingDirectories.length]);
                WALStorage walStorage = walStorageProvider.create(workingDirectory, domain, regionName, walReplicator);
                regionStore = new RegionStore(walStorage, rwoChanges);
                regionStore.load();
                regionStores.put(regionName, regionStore);
            }
            return regionStore;
        }
    }

    public Set<Map.Entry<RegionName, RegionStore>> getAll() {
        return regionStores.entrySet();
    }
}
