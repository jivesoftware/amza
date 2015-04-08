package com.jivesoftware.os.amza.berkeleydb;

import com.jivesoftware.os.amza.shared.AmzaVersionConstants;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.WALIndexProvider;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import java.io.File;

/**
 *
 */
public class BerkeleyDBWALIndexProvider implements WALIndexProvider<BerkeleyDBWALIndex> {

    private final Environment[] environments;

    public BerkeleyDBWALIndexProvider(String[] baseDirs, int stripes) {
        this.environments = new Environment[stripes];
        for (int i = 0; i < environments.length; i++) {
            File active = new File(
                new File(
                    new File(baseDirs[i % baseDirs.length], AmzaVersionConstants.LATEST_VERSION),
                    "berkeleydb"),
                String.valueOf(i));
            active.mkdirs();

            // Open the environment, creating one if it does not exist
            EnvironmentConfig envConfig = new EnvironmentConfig()
                .setAllowCreate(true)
                .setSharedCache(true);
            this.environments[i] = new Environment(active, envConfig);
        }
    }

    @Override

    public BerkeleyDBWALIndex createIndex(RegionName regionName) throws Exception {
        return new BerkeleyDBWALIndex(environments[Math.abs(regionName.hashCode()) % environments.length], "active", regionName.toBase64());
    }
}
