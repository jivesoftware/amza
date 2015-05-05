package com.jivesoftware.os.amza.shared;

/**
 *
 * @author jonathan.colt
 */
public class NoOpWALIndexProvider implements WALIndexProvider<NoOpWALIndex> {

    @Override
    public NoOpWALIndex createIndex(RegionName regionName) throws Exception {
        return new NoOpWALIndex();
    }

    @Override
    public void deleteIndex(RegionName regionName) throws Exception {
    }

}
