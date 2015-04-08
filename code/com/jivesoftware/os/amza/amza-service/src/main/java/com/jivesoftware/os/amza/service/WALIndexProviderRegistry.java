package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.shared.MemoryWALIndex;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.WALIndexProvider;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author jonathan.colt
 */
public class WALIndexProviderRegistry {

    private final ConcurrentHashMap<String, WALIndexProvider<?>> registry = new ConcurrentHashMap<>();

    public WALIndexProviderRegistry() {
        register("memory", new WALIndexProvider<MemoryWALIndex>() {

            @Override
            public MemoryWALIndex createIndex(RegionName regionName) throws Exception {
                return new MemoryWALIndex();
            }
        });
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
