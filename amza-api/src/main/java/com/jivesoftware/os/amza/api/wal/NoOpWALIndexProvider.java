package com.jivesoftware.os.amza.api.wal;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;

/**
 *
 * @author jonathan.colt
 */
public class NoOpWALIndexProvider implements WALIndexProvider<NoOpWALIndex> {

    private final String name;

    public NoOpWALIndexProvider(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public NoOpWALIndex createIndex(VersionedPartitionName versionedPartitionName) throws Exception {
        return new NoOpWALIndex(name);
    }

    @Override
    public void deleteIndex(VersionedPartitionName versionedPartitionName) throws Exception {
    }

    @Override
    public void flush(Iterable<NoOpWALIndex> partitionNames, boolean fsync) throws Exception {
    }
}
