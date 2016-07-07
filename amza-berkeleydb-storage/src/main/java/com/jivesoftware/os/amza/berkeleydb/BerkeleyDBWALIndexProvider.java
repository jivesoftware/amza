package com.jivesoftware.os.amza.berkeleydb;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.AmzaVersionConstants;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.wal.WALIndexProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import java.io.File;
import java.util.Set;

/**
 *
 */
public class BerkeleyDBWALIndexProvider implements WALIndexProvider<BerkeleyDBWALIndex> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static final String INDEX_CLASS_NAME = "berkeleydb";

    private final String name;
    private final Environment[] environments;

    public BerkeleyDBWALIndexProvider(String name, int numberOfStripes, File[] baseDirs) {
        this.name = name;
        this.environments = new Environment[numberOfStripes];
        for (int i = 0; i < environments.length; i++) {
            File active = new File(new File(new File(baseDirs[i % baseDirs.length], AmzaVersionConstants.LATEST_VERSION), INDEX_CLASS_NAME), String.valueOf(i));
            if (!active.exists() && !active.mkdirs()) {
                throw new RuntimeException("Failed while trying to mkdirs for " + active);
            }

            // Open the environment, creating one if it does not exist
            EnvironmentConfig envConfig = new EnvironmentConfig()
                //.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false")
                .setAllowCreate(true)
                .setSharedCache(true);
            envConfig.setCachePercentVoid(30);
            this.environments[i] = new Environment(active, envConfig);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public BerkeleyDBWALIndex createIndex(VersionedPartitionName versionedPartitionName, int maxValueSizeInIndex, int stripe) throws Exception {
        BerkeleyDBWALIndexName indexName = new BerkeleyDBWALIndexName(BerkeleyDBWALIndexName.Type.active, versionedPartitionName.toBase64());
        return new BerkeleyDBWALIndex(name, versionedPartitionName, environments, indexName, stripe);
    }

    @Override
    public void deleteIndex(VersionedPartitionName versionedPartitionName, int stripe) throws Exception {
        BerkeleyDBWALIndexName name = new BerkeleyDBWALIndexName(BerkeleyDBWALIndexName.Type.active, versionedPartitionName.toBase64());
        Environment env = environments[stripe];
        for (BerkeleyDBWALIndexName n : name.all()) {
            try {
                env.removeDatabase(null, n.getPrimaryName());
                LOG.info("Removed database: {}", n.getPrimaryName());
            } catch (DatabaseNotFoundException x) {
                // ignore
            }
            try {
                env.removeDatabase(null, n.getPrefixName());
                LOG.info("Removed database: {}", n.getPrefixName());
            } catch (DatabaseNotFoundException x) {
                // ignore
            }
        }
    }

    @Override
    public void flush(Iterable<BerkeleyDBWALIndex> indexes, boolean fsync) throws Exception {
        Set<Integer> stripes = Sets.newHashSet();
        for (BerkeleyDBWALIndex index : indexes) {
            stripes.add(index.getStripe());
        }
        for (int stripe : stripes) {
            environments[stripe].flushLog(fsync);
        }
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

}
