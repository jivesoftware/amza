package com.jivesoftware.os.amza.berkeleydb;

import com.jivesoftware.os.amza.shared.AmzaVersionConstants;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.WALIndex;
import com.jivesoftware.os.amza.shared.WALIndexProvider;
import java.io.File;

/**
 *
 */
public class BerkeleyDBWALIndexProvider implements WALIndexProvider {

    private final String[] baseDirs;

    public BerkeleyDBWALIndexProvider(String[] baseDirs) {
        this.baseDirs = baseDirs;
    }

    @Override
    public WALIndex createIndex(RegionName regionName) throws Exception {
        File baseRegionDir = new File(new File(baseDirs[Math.abs(regionName.hashCode()) % baseDirs.length]),
            "bkdb" + File.separator + regionName.getRegionName() + "-" + regionName.getRingName());
        File regionDir = new File(baseRegionDir, AmzaVersionConstants.LATEST_VERSION);
        return new BerkeleyDBWALIndex(regionDir, regionName);
    }
}
