package com.jivesoftware.os.amza.shared;

/**
 *
 * @author jonathan.colt
 */
public class MemoryWALIndexProvider implements WALIndexProvider<MemoryWALIndex> {

    @Override
    public MemoryWALIndex createIndex(RegionName regionName) throws Exception {
        return new MemoryWALIndex();
    }

    @Override
    public void deleteIndex(RegionName regionName) throws Exception {
    }
}
