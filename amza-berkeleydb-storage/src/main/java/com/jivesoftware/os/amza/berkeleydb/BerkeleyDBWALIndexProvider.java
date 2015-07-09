package com.jivesoftware.os.amza.berkeleydb;

import com.jivesoftware.os.amza.shared.AmzaVersionConstants;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.wal.WALIndexProvider;
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
            if (!active.exists() && !active.mkdirs()) {
                throw new RuntimeException("Failed while trying to mkdirs for " + active);
            }

            // Open the environment, creating one if it does not exist
            EnvironmentConfig envConfig = new EnvironmentConfig()
                .setAllowCreate(true)
                .setSharedCache(true);
            envConfig.setCachePercentVoid(30);
            this.environments[i] = new Environment(active, envConfig);
        }
    }

    @Override
    public BerkeleyDBWALIndex createIndex(VersionedPartitionName versionedPartitionName) throws Exception {
        BerkeleyDBWALIndexName name = new BerkeleyDBWALIndexName(BerkeleyDBWALIndexName.Prefix.active, versionedPartitionName.toBase64());
        return new BerkeleyDBWALIndex(environments[Math.abs(versionedPartitionName.hashCode() % environments.length)], name);
    }

    @Override
    public void deleteIndex(VersionedPartitionName versionedPartitionName) throws Exception {
        BerkeleyDBWALIndexName name = new BerkeleyDBWALIndexName(BerkeleyDBWALIndexName.Prefix.active, versionedPartitionName.toBase64());
        for (BerkeleyDBWALIndexName n : name.all()) {
            environments[Math.abs(versionedPartitionName.hashCode() % environments.length)].removeDatabase(null, n.getName());
        }
    }

}
