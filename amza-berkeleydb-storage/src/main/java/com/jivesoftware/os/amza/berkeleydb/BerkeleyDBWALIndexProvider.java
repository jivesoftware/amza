package com.jivesoftware.os.amza.berkeleydb;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.AmzaVersionConstants;
import com.jivesoftware.os.amza.api.partition.PartitionStripeFunction;
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
    private final PartitionStripeFunction partitionStripeFunction;
    private final Environment[] environments;

    public BerkeleyDBWALIndexProvider(String name, PartitionStripeFunction partitionStripeFunction, File[] baseDirs) {
        this.name = name;
        this.partitionStripeFunction = partitionStripeFunction;
        this.environments = new Environment[partitionStripeFunction.getNumberOfStripes()];
        for (int i = 0; i < environments.length; i++) {
            File active = new File(
                new File(
                    new File(baseDirs[i % baseDirs.length], AmzaVersionConstants.LATEST_VERSION), INDEX_CLASS_NAME),
                String.valueOf(i));
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

    private Environment getEnvironment(VersionedPartitionName versionedPartitionName) {
        return environments[partitionStripeFunction.stripe(versionedPartitionName.getPartitionName())];
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public BerkeleyDBWALIndex createIndex(VersionedPartitionName versionedPartitionName, int maxUpdatesBetweenCompactionHintMarker) throws Exception {
        BerkeleyDBWALIndexName indexName = new BerkeleyDBWALIndexName(BerkeleyDBWALIndexName.Type.active, versionedPartitionName.toBase64());
        return new BerkeleyDBWALIndex(name, versionedPartitionName, getEnvironment(versionedPartitionName), indexName);
    }

    @Override
    public void deleteIndex(VersionedPartitionName versionedPartitionName) throws Exception {
        BerkeleyDBWALIndexName name = new BerkeleyDBWALIndexName(BerkeleyDBWALIndexName.Type.active, versionedPartitionName.toBase64());
        Environment env = getEnvironment(versionedPartitionName);
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
            stripes.add(partitionStripeFunction.stripe(index.getVersionedPartitionName().getPartitionName()));
        }
        for (int stripe : stripes) {
            environments[stripe].flushLog(fsync);
        }
    }

}
