package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.shared.region.VersionedRegionName;

/**
 *
 * @author jonathan.colt
 */
public class NoOpWALIndexProvider implements WALIndexProvider<NoOpWALIndex> {

    @Override
    public NoOpWALIndex createIndex(VersionedRegionName versionedRegionName) throws Exception {
        return new NoOpWALIndex();
    }

    @Override
    public void deleteIndex(VersionedRegionName versionedRegionName) throws Exception {
    }

}
