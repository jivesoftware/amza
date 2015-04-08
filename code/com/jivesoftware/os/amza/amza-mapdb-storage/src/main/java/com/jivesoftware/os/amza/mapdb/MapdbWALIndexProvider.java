package com.jivesoftware.os.amza.mapdb;

import com.jivesoftware.os.amza.shared.AmzaVersionConstants;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.WALIndexProvider;
import java.io.File;

/**
 *
 */
public class MapdbWALIndexProvider implements WALIndexProvider<MapdbWALIndex> {

    private final File[] baseDirs;

    public MapdbWALIndexProvider(String[] workingDirs) {
        this.baseDirs = new File[workingDirs.length];
        for (int i = 0; i < baseDirs.length; i++) {
            baseDirs[i] = new File(
                new File(baseDirs[i % baseDirs.length], AmzaVersionConstants.LATEST_VERSION),
                "mapdb");
        }
    }

    @Override
    public MapdbWALIndex createIndex(RegionName regionName) throws Exception {
        File baseDir = baseDirs[Math.abs(regionName.hashCode()) % baseDirs.length];
        File regionDir = new File(baseDir, regionName.toBase64());
        return new MapdbWALIndex(regionDir, regionName);
    }
}
