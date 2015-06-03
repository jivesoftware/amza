package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.shared.region.VersionedRegionName;

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
