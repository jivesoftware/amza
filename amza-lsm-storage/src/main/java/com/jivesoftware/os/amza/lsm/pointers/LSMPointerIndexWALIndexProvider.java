package com.jivesoftware.os.amza.lsm.pointers;

import com.jivesoftware.os.amza.api.AmzaVersionConstants;
import com.jivesoftware.os.amza.api.partition.PartitionStripeFunction;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.wal.WALIndexProvider;
import com.jivesoftware.os.amza.lsm.pointers.LSMPointerIndexWALIndexName.Type;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.sleepycat.je.DatabaseNotFoundException;
import java.io.File;

/**
 *
 */
public class LSMPointerIndexWALIndexProvider implements WALIndexProvider<LSMPointerIndexWALIndex> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    public static final String INDEX_CLASS_NAME = "lsmpointerindex";

    private final String name;
    private final PartitionStripeFunction partitionStripeFunction;
    private final LSMPointerIndexEnvironment[] environments;

    public LSMPointerIndexWALIndexProvider(String name, PartitionStripeFunction partitionStripeFunction, File[] baseDirs) {
        this.name = name;
        this.partitionStripeFunction = partitionStripeFunction;
        this.environments = new LSMPointerIndexEnvironment[partitionStripeFunction.getNumberOfStripes()];
        for (int i = 0; i < environments.length; i++) {
            File active = new File(
                new File(
                    new File(baseDirs[i % baseDirs.length], AmzaVersionConstants.LATEST_VERSION),
                    INDEX_CLASS_NAME),
                String.valueOf(i));
            if (!active.exists() && !active.mkdirs()) {
                throw new RuntimeException("Failed while trying to mkdirs for " + active);
            }
            this.environments[i] = new LSMPointerIndexEnvironment(active);
        }
    }

    private LSMPointerIndexEnvironment getEnvironment(VersionedPartitionName versionedPartitionName) {
        return environments[partitionStripeFunction.stripe(versionedPartitionName.getPartitionName())];
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public LSMPointerIndexWALIndex createIndex(VersionedPartitionName versionedPartitionName) throws Exception {
        LSMPointerIndexWALIndexName indexName = new LSMPointerIndexWALIndexName(Type.active, versionedPartitionName.toBase64());
        //TODO config flush interval
        return new LSMPointerIndexWALIndex(name,
            versionedPartitionName,
            getEnvironment(versionedPartitionName),
            indexName,
            100_000);
    }

    @Override
    public void deleteIndex(VersionedPartitionName versionedPartitionName) throws Exception {
        LSMPointerIndexWALIndexName name = new LSMPointerIndexWALIndexName(LSMPointerIndexWALIndexName.Type.active, versionedPartitionName.toBase64());
        LSMPointerIndexEnvironment env = getEnvironment(versionedPartitionName);
        for (LSMPointerIndexWALIndexName n : name.all()) {
            try {
                env.remove(n.getPrimaryName());
                LOG.info("Removed database: {}", n.getPrimaryName());
            } catch (DatabaseNotFoundException x) {
                // ignore
            }
            try {
                env.remove(n.getPrefixName());
                LOG.info("Removed database: {}", n.getPrefixName());
            } catch (DatabaseNotFoundException x) {
                // ignore
            }
        }
    }

    @Override
    public void flush(Iterable<LSMPointerIndexWALIndex> indexes, boolean fsync) throws Exception {
        for (LSMPointerIndexWALIndex index : indexes) {
            index.flush(fsync); // So call me maybe?
        }
    }

}
