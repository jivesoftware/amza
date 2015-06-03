package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.shared.wal.MemoryWALIndexProvider;
import com.jivesoftware.os.amza.shared.wal.NoOpWALIndexProvider;
import com.jivesoftware.os.amza.shared.wal.WALIndexProvider;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author jonathan.colt
 */
public class WALIndexProviderRegistry {

    private final ConcurrentHashMap<String, WALIndexProvider<?>> registry = new ConcurrentHashMap<>();

    public WALIndexProviderRegistry() {
        register("memory", new MemoryWALIndexProvider());
        register("noop", new NoOpWALIndexProvider());

    }

    public WALIndexProvider<?> getWALIndexProvider(WALStorageDescriptor storageDescriptor) throws Exception {
        // TODO figure out how to get storageDescriptor into WALIndexProvider
        // TODO add loading of WALIndexProvider based on classForName crap! (We love plugins)
        return registry.get(storageDescriptor.primaryIndexDescriptor.className);
    }

    final public void register(String name, WALIndexProvider<?> indexProvider) {
        registry.put(name, indexProvider);
    }
}
