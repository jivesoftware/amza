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

import com.google.common.collect.ImmutableSet;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageProvider;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class WALs {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int SEMAPHORE_PERMITS = 64;

    private final String[] workingDirectories;
    private final String storeName;
    private final WALStorageProvider walStorageProvider;
    private final ConcurrentHashMap<RegionName, WALStorage> walStores = new ConcurrentHashMap<>();
    private final StripingLocksProvider locksProvider = new StripingLocksProvider(1024); // TODO expose to config
    private final Semaphore[] semaphores = new Semaphore[1024];

    public WALs(String[] workingDirectories,
        String storeName,
        WALStorageProvider rowStorageProvider) {
        this.workingDirectories = workingDirectories;
        this.storeName = storeName;
        this.walStorageProvider = rowStorageProvider;
        for (int i = 0; i < semaphores.length; i++) {
            semaphores[i] = new Semaphore(SEMAPHORE_PERMITS, true);
        }
    }

    public String getName() {
        return storeName;
    }

    private Semaphore getSemaphore(RegionName regionName) {
        return semaphores[Math.abs(regionName.hashCode()) % semaphores.length];
    }

    public void load() throws Exception {
        Set<RegionName> regionNames = walStorageProvider.listExisting(workingDirectories, storeName);
        for (RegionName regionName : regionNames) {
            try {
                getStorage(regionName);
            } catch (Exception x) {
                LOG.warn("Failed to load storage for region {}", new Object[]{regionName}, x);
            }
        }
    }

    public <R> R execute(RegionName regionName, Tx<R> tx) throws Exception {
        Semaphore semaphore = getSemaphore(regionName);
        semaphore.acquire();
        try {
            return tx.execute(getStorage(regionName));
        } finally {
            semaphore.release();
        }
    }

    private WALStorage getStorage(RegionName regionName) throws Exception {
        synchronized (locksProvider.lock(regionName, 1234)) {
            WALStorage storage = walStores.get(regionName);
            if (storage == null) {
                File workingDirectory = new File(workingDirectories[Math.abs(regionName.hashCode()) % workingDirectories.length]);
                storage = walStorageProvider.create(workingDirectory, storeName, regionName, null);
                storage.load();
                walStores.put(regionName, storage);
            }
            return storage;
        }
    }

    public void removeIfEmpty(RegionName regionName) throws Exception {
        Semaphore semaphore = getSemaphore(regionName);
        semaphore.acquire(SEMAPHORE_PERMITS);
        try {
            WALStorage storage = walStores.get(regionName);
            if (storage != null && storage.delete(true)) {
                walStores.remove(regionName);
            }
        } finally {
            semaphore.release(SEMAPHORE_PERMITS);
        }
    }

    public Set<RegionName> getAllRegions() {
        return ImmutableSet.copyOf(walStores.keySet());
    }

    public interface Tx<R> {

        R execute(WALStorage storage) throws Exception;
    }
}
