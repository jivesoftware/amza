package com.jivesoftware.os.amza.service;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.wal.MemoryWALIndexProvider;
import com.jivesoftware.os.amza.api.wal.NoOpWALIndexProvider;
import com.jivesoftware.os.amza.api.wal.WALIndexProvider;
import com.jivesoftware.os.amza.service.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.MemoryBackedRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import java.util.Map;

/**
 * @author jonathan.colt
 */
public class WALIndexProviderRegistry {

    private final Map<String, WALIndexProvider<?>> indexRegistry = Maps.newConcurrentMap();
    private final Map<String, RowIOProvider> rowIORegistry = Maps.newConcurrentMap();

    public WALIndexProviderRegistry(MemoryBackedRowIOProvider ephemeralRowIOProvider, BinaryRowIOProvider persistentRowIOProvider) {
        //public WALIndexProviderRegistry(String[] workingDirectories, IoStats ioStats, int corruptionParanoiaFactor, boolean useMemMap) {
        register(new MemoryWALIndexProvider("memory_ephemeral"), ephemeralRowIOProvider);
        register(new MemoryWALIndexProvider("memory_persistent"), persistentRowIOProvider);
        register(new NoOpWALIndexProvider("noop_persistent"), persistentRowIOProvider);
    }

    public WALIndexProvider<?> getWALIndexProvider(String name) throws Exception {
        // TODO figure out how to get storageDescriptor into WALIndexProvider
        // TODO add loading of WALIndexProvider based on classForName crap! (We love plugins)
        return indexRegistry.get(name);
    }

    public RowIOProvider getRowIOProvider(String name) throws Exception {
        return rowIORegistry.get(name);
    }

    final public void register(WALIndexProvider<?> indexProvider, RowIOProvider rowIOProvider) {
        indexRegistry.put(indexProvider.getName(), indexProvider);
        rowIORegistry.put(indexProvider.getName(), rowIOProvider);
    }

    public void start() {
        for (WALIndexProvider<?> provider : indexRegistry.values()) {
            provider.start();
        }
    }

    public void stop() {
        for (WALIndexProvider<?> provider : indexRegistry.values()) {
            provider.stop();
        }
    }
}
