package com.jivesoftware.os.amza.lab.pointers;

import com.jivesoftware.os.amza.api.AmzaVersionConstants;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.wal.WALIndexProvider;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexWALIndexName.Type;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public class LABPointerIndexWALIndexProvider implements WALIndexProvider<LABPointerIndexWALIndex> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    public static final String INDEX_CLASS_NAME = "lab";

    private final String name;
    private final LABEnvironment[] environments;
    private final LABPointerIndexConfig config;
    private final LRUConcurrentBAHLinkedHash<Leaps> leapCache;

    public LABPointerIndexWALIndexProvider(LABPointerIndexConfig config,
        String name,
        int numberOfStripes,
        File[] baseDirs) {
        this.config = config;

        this.name = name;
        this.environments = new LABEnvironment[numberOfStripes];

        ExecutorService compactorThreadPool = LABEnvironment.buildLABCompactorThreadPool(config.getConcurrency());
        ExecutorService destroyThreadPool = LABEnvironment.buildLABDestroyThreadPool(environments.length);
        leapCache = LABEnvironment.buildLeapsCache((int)config.getLeapCacheMaxCapacity(), config.getConcurrency());

        for (int i = 0; i < environments.length; i++) {
            File active = new File(
                new File(
                    new File(baseDirs[i % baseDirs.length], AmzaVersionConstants.LATEST_VERSION),
                    INDEX_CLASS_NAME),
                String.valueOf(i));
            if (!active.exists() && !active.mkdirs()) {
                throw new RuntimeException("Failed while trying to mkdirs for " + active);
            }
            this.environments[i] = new LABEnvironment(compactorThreadPool,
                destroyThreadPool,
                active,
                config.getUseMemMap(),
                config.getMinMergeDebt(),
                config.getMaxMergeDebt(),
                leapCache);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public LABPointerIndexWALIndex createIndex(VersionedPartitionName versionedPartitionName, int stripe) throws Exception {
        LABPointerIndexWALIndexName indexName = new LABPointerIndexWALIndexName(Type.active, versionedPartitionName.toBase64());
        //TODO config flush interval
        return new LABPointerIndexWALIndex(name,
            versionedPartitionName,
            environments,
            stripe,
            indexName,
            config);
    }

    @Override
    public void deleteIndex(VersionedPartitionName versionedPartitionName, int stripe) throws Exception {
        LABPointerIndexWALIndexName name = new LABPointerIndexWALIndexName(LABPointerIndexWALIndexName.Type.active, versionedPartitionName.toBase64());
        LABEnvironment env = environments[stripe];
        for (LABPointerIndexWALIndexName n : name.all()) {
            env.remove(n.getPrimaryName());
            LOG.info("Removed database: {}", n.getPrimaryName());
            env.remove(n.getPrefixName());
            LOG.info("Removed database: {}", n.getPrefixName());
        }
    }

    @Override
    public void flush(Iterable<LABPointerIndexWALIndex> indexes, boolean fsync) throws Exception {
        for (LABPointerIndexWALIndex index : indexes) {
            index.flush(fsync); // So call me maybe?
        }
    }

}
