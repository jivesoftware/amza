package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.service.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.MemoryBackedRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import com.jivesoftware.os.amza.shared.wal.MemoryWALIndexProvider;
import com.jivesoftware.os.amza.shared.wal.NoOpWALIndexProvider;
import com.jivesoftware.os.amza.shared.wal.WALIndexProvider;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jonathan.colt
 */
public class WALIndexProviderRegistry {

    private final ConcurrentHashMap<String, WALIndexProvider<?>> indexRegistry = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RowIOProvider<?>> rowIORegistry = new ConcurrentHashMap<>();

    public WALIndexProviderRegistry(MemoryBackedRowIOProvider ephemeralRowIOProvider, BinaryRowIOProvider persistentRowIOProvider) {
    //public WALIndexProviderRegistry(String[] workingDirectories, IoStats ioStats, int corruptionParanoiaFactor, boolean useMemMap) {
        register("memory_ephemeral", new MemoryWALIndexProvider(), ephemeralRowIOProvider);
        register("memory_persistent", new MemoryWALIndexProvider(), persistentRowIOProvider);
        register("noop_persistent", new NoOpWALIndexProvider(), persistentRowIOProvider);
    }

    public WALIndexProvider<?> getWALIndexProvider(WALStorageDescriptor storageDescriptor) throws Exception {
        // TODO figure out how to get storageDescriptor into WALIndexProvider
        // TODO add loading of WALIndexProvider based on classForName crap! (We love plugins)
        return indexRegistry.get(storageDescriptor.primaryIndexDescriptor.className);
    }

    public RowIOProvider<?> getRowIOProvider(WALStorageDescriptor storageDescriptor) throws Exception {
        return rowIORegistry.get(storageDescriptor.primaryIndexDescriptor.className);
    }

    final public void register(String name, WALIndexProvider<?> indexProvider, RowIOProvider<?> rowIOProvider) {
        indexRegistry.put(name, indexProvider);
        rowIORegistry.put(name, rowIOProvider);
    }
}
