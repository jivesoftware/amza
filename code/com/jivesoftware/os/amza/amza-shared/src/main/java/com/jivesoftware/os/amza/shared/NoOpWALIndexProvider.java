package com.jivesoftware.os.amza.shared;

/**
 *
 * @author jonathan.colt
 */
public class NoOpWALIndexProvider implements WALIndexProvider {

    @Override
    public WALIndex createIndex(RegionName regionName) throws Exception {
        return new NoOpWALIndex();
    }

}
