package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;

/**
 *
 * @author jonathan.colt
 */
public class MemoryWALIndexProvider implements WALIndexProvider<MemoryWALIndex> {

    @Override
    public MemoryWALIndex createIndex(VersionedPartitionName versionedPartitionName) throws Exception {
        return new MemoryWALIndex();
    }

    @Override
    public void deleteIndex(VersionedPartitionName versionedPartitionName) throws Exception {
    }
}
