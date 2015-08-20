package com.jivesoftware.os.amza.service.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan.colt
 */
public class PartitionComposter {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private ScheduledExecutorService scheduledThreadPool;

    private final AmzaStats amzaStats;
    private final PartitionIndex partitionIndex;
    private final PartitionCreator partitionProvider;
    private final AmzaRingStoreReader amzaRingReader;
    private final PartitionStripeProvider partitionStripeProvider;
    private final PartitionStatusStorage partitionStatusStorage;

    public PartitionComposter(AmzaStats amzaStats,
        PartitionIndex partitionIndex,
        PartitionCreator partitionProvider,
        AmzaRingStoreReader amzaRingReader,
        PartitionStatusStorage partitionMemberStatusStorage,
        PartitionStripeProvider partitionStripeProvider) {

        this.amzaStats = amzaStats;
        this.partitionIndex = partitionIndex;
        this.partitionProvider = partitionProvider;
        this.amzaRingReader = amzaRingReader;
        this.partitionStatusStorage = partitionMemberStatusStorage;
        this.partitionStripeProvider = partitionStripeProvider;
    }

    public void start() throws Exception {

        scheduledThreadPool = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("partition-composter-%d").build());
        scheduledThreadPool.scheduleWithFixedDelay(() -> {
            try {
                compost();
            } catch (Exception x) {
                LOG.debug("Failing to compact tombstones.", x);

            }
        }, 0, 1, TimeUnit.MINUTES); // TODO config
    }

    public void stop() throws Exception {
        this.scheduledThreadPool.shutdownNow();
        this.scheduledThreadPool = null;
    }

    public void compost() throws Exception {
        List<VersionedPartitionName> composted = new ArrayList<>();
        partitionStatusStorage.streamLocalState((partitionName, ringMember, versionedStatus) -> {
            if (versionedStatus.status == Status.EXPUNGE) {
                try {
                    amzaStats.beginCompaction("Expunge " + partitionName + " " + versionedStatus);
                    partitionStripeProvider.txPartition(partitionName, (PartitionStripe stripe, HighwaterStorage highwaterStorage) -> {
                        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, versionedStatus.version);
                        if (stripe.expungePartition(versionedPartitionName)) {
                            partitionIndex.remove(versionedPartitionName);
                            highwaterStorage.expunge(versionedPartitionName);
                            partitionProvider.destroyPartition(partitionName);
                            composted.add(versionedPartitionName);
                        }
                        return null;
                    });
                } finally {
                    amzaStats.endCompaction("Expunge " + partitionName + " " + versionedStatus);
                }

            } else if (!amzaRingReader.isMemberOfRing(partitionName.getRingName()) || !partitionProvider.hasPartition(partitionName)) {
                partitionStatusStorage.markForDisposal(new VersionedPartitionName(partitionName, versionedStatus.version), ringMember);
            }
            return true;
        });
        partitionStatusStorage.expunged(composted);

    }

}
