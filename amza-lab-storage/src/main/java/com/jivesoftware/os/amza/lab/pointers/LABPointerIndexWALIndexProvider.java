package com.jivesoftware.os.amza.lab.pointers;

import com.jivesoftware.os.amza.api.AmzaVersionConstants;
import com.jivesoftware.os.amza.api.partition.PartitionStripeFunction;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.wal.WALIndexProvider;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexWALIndexName.Type;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.sleepycat.je.DatabaseNotFoundException;
import java.io.File;

/**
 *
 */
public class LABPointerIndexWALIndexProvider implements WALIndexProvider<LABPointerIndexWALIndex> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    public static final String INDEX_CLASS_NAME = "labpointerindex";

    private final String name;
    private final PartitionStripeFunction partitionStripeFunction;
    private final LABPointerIndexEnvironment[] environments;

    public LABPointerIndexWALIndexProvider(String name, PartitionStripeFunction partitionStripeFunction, File[] baseDirs, int maxMergeDebt) {
        this.name = name;
        this.partitionStripeFunction = partitionStripeFunction;
        this.environments = new LABPointerIndexEnvironment[partitionStripeFunction.getNumberOfStripes()];
        for (int i = 0; i < environments.length; i++) {
            File active = new File(
                new File(
                    new File(baseDirs[i % baseDirs.length], AmzaVersionConstants.LATEST_VERSION),
                    INDEX_CLASS_NAME),
                String.valueOf(i));
            if (!active.exists() && !active.mkdirs()) {
                throw new RuntimeException("Failed while trying to mkdirs for " + active);
            }
            this.environments[i] = new LABPointerIndexEnvironment(active, maxMergeDebt);
        }
    }

    private LABPointerIndexEnvironment getEnvironment(VersionedPartitionName versionedPartitionName) {
        return environments[partitionStripeFunction.stripe(versionedPartitionName.getPartitionName())];
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public LABPointerIndexWALIndex createIndex(VersionedPartitionName versionedPartitionName) throws Exception {
        LABPointerIndexWALIndexName indexName = new LABPointerIndexWALIndexName(Type.active, versionedPartitionName.toBase64());
        //TODO config flush interval
        return new LABPointerIndexWALIndex(name,
            versionedPartitionName,
            getEnvironment(versionedPartitionName),
            indexName,
            100_000);
    }

    @Override
    public void deleteIndex(VersionedPartitionName versionedPartitionName) throws Exception {
        LABPointerIndexWALIndexName name = new LABPointerIndexWALIndexName(LABPointerIndexWALIndexName.Type.active, versionedPartitionName.toBase64());
        LABPointerIndexEnvironment env = getEnvironment(versionedPartitionName);
        for (LABPointerIndexWALIndexName n : name.all()) {
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
    public void flush(Iterable<LABPointerIndexWALIndex> indexes, boolean fsync) throws Exception {
        for (LABPointerIndexWALIndex index : indexes) {
            index.flush(fsync); // So call me maybe?
        }
    }

}
