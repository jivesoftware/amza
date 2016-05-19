package com.jivesoftware.os.amza.api.wal;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;

/**
 *
 * @author jonathan.colt
 */
public class MemoryWALIndexProvider implements WALIndexProvider<MemoryWALIndex> {

    private final String name;

    public MemoryWALIndexProvider(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MemoryWALIndex createIndex(VersionedPartitionName versionedPartitionName, int stripe) throws Exception {
        return new MemoryWALIndex(name, stripe);
    }

    @Override
    public void deleteIndex(VersionedPartitionName versionedPartitionName, int stripe) throws Exception {
    }

    @Override
    public void flush(Iterable<MemoryWALIndex> indexes, boolean fsync) throws Exception {
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
}
