package com.jivesoftware.os.amza.shared;

/**
 *
 * @author jonathan.colt
 */
public class MemoryWALIndexProvider implements WALIndexProvider<MemoryWALIndex> {

    @Override
    public MemoryWALIndex createIndex(VersionedRegionName versionedRegionName) throws Exception {
        return new MemoryWALIndex();
    }

    @Override
    public void deleteIndex(VersionedRegionName versionedRegionName) throws Exception {
    }
}
