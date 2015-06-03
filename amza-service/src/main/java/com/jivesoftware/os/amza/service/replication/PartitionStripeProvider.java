package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 * @author jonathan.colt
 */
public class PartitionStripeProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final PartitionStripe systemStripe;
    private final PartitionStripe[] deltaStripes;

    public PartitionStripeProvider(PartitionStripe systemStripe, PartitionStripe[] stripes) {
        this.systemStripe = systemStripe;
        this.deltaStripes = stripes;
    }

    public PartitionStripe getSystemPartitionStripe() {
        return systemStripe;
    }

    public PartitionStripe getPartitionStripe(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return systemStripe;
        }
        return deltaStripes[Math.abs(partitionName.hashCode()) % deltaStripes.length];
    }

    public void removePartition(VersionedPartitionName versionedPartitionName) throws Exception {
        if (!versionedPartitionName.getPartitionName().isSystemPartition()) {
            deltaStripes[Math.abs(versionedPartitionName.getPartitionName().hashCode()) % deltaStripes.length].expungePartition(versionedPartitionName);
        }
    }
}
