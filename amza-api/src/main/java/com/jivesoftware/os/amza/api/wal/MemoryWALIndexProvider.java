package com.jivesoftware.os.amza.api.wal;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;

/**
 *
 * @author jonathan.colt
 */
public class MemoryWALIndexProvider implements WALIndexProvider<MemoryWALIndex> {

    @Override
    public MemoryWALIndex createIndex(VersionedPartitionName versionedPartitionName, int maxUpdatesBetweenCompactionHintMarker) throws Exception {
        return new MemoryWALIndex();
    }

    @Override
    public void deleteIndex(VersionedPartitionName versionedPartitionName) throws Exception {
    }
}
