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
import com.jivesoftware.os.amza.service.IndexedWALStorageProvider;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class WALs {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int SEMAPHORE_PERMITS = 64;

    private final String[] workingDirectories;
    private final String storeName;
    private final IndexedWALStorageProvider walStorageProvider;
    private final ConcurrentHashMap<VersionedPartitionName, WALStorage> walStores = new ConcurrentHashMap<>();
    private final StripingLocksProvider<VersionedPartitionName> locksProvider = new StripingLocksProvider<>(1024); // TODO expose to config
    private final Semaphore[] semaphores = new Semaphore[1024];

    public WALs(String[] workingDirectories,
        String storeName,
        IndexedWALStorageProvider rowStorageProvider) {
        this.workingDirectories = Arrays.copyOf(workingDirectories, workingDirectories.length);
        this.storeName = storeName;
        this.walStorageProvider = rowStorageProvider;
        for (int i = 0; i < semaphores.length; i++) {
            semaphores[i] = new Semaphore(SEMAPHORE_PERMITS, true);
        }
    }

    public String getName() {
        return storeName;
    }

    private Semaphore getSemaphore(VersionedPartitionName versionedPartitionName) {
        return semaphores[(int) Math.abs((long) versionedPartitionName.hashCode()) % semaphores.length];
    }

    public void load() throws Exception {
        Set<VersionedPartitionName> versionedPartitionNames = walStorageProvider.listExisting(workingDirectories, storeName);
        for (VersionedPartitionName versionedPartitionName : versionedPartitionNames) {
            try {
                getStorage(versionedPartitionName);
            } catch (Exception x) {
                LOG.warn("Failed to load storage for partition {}", new Object[]{versionedPartitionName}, x);
            }
        }
    }

    public <R> R execute(VersionedPartitionName versionedPartitionName, Tx<R> tx) throws Exception {
        Semaphore semaphore = getSemaphore(versionedPartitionName);
        semaphore.acquire();
        try {
            return tx.execute(getStorage(versionedPartitionName));
        } finally {
            semaphore.release();
        }
    }

    private WALStorage getStorage(VersionedPartitionName versionedPartitionName) throws Exception {
        synchronized (locksProvider.lock(versionedPartitionName, 1234)) {
            return walStores.computeIfAbsent(versionedPartitionName, (key) -> {
                try {
                    File workingDirectory = new File(workingDirectories[(int) Math.abs((long) key.hashCode()) % workingDirectories.length]);
                    WALStorage storage = walStorageProvider.create(workingDirectory, storeName, key, null);
                    storage.load();
                    return storage;
                } catch (Exception x) {
                    throw new RuntimeException(x);
                }
            });
        }
    }

    public void removeIfEmpty(VersionedPartitionName versionedPartitionName) throws Exception {
        Semaphore semaphore = getSemaphore(versionedPartitionName);
        semaphore.acquire(SEMAPHORE_PERMITS);
        try {
            WALStorage storage = walStores.get(versionedPartitionName);
            if (storage != null && storage.delete(true)) {
                walStores.remove(versionedPartitionName);
            }
        } finally {
            semaphore.release(SEMAPHORE_PERMITS);
        }
    }

    public Set<VersionedPartitionName> getAllPartitions() {
        return ImmutableSet.copyOf(walStores.keySet());
    }

    public interface Tx<R> {

        R execute(WALStorage storage) throws Exception;
    }
}
