package com.jivesoftware.os.amza.service.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.stats.AmzaStats.CompactionFamily;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.aquarium.State;
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
    private final PartitionStateStorage partitionStateStorage;

    public PartitionComposter(AmzaStats amzaStats,
        PartitionIndex partitionIndex,
        PartitionCreator partitionProvider,
        AmzaRingStoreReader amzaRingReader,
        PartitionStateStorage partitionStateStorage,
        PartitionStripeProvider partitionStripeProvider) {

        this.amzaStats = amzaStats;
        this.partitionIndex = partitionIndex;
        this.partitionProvider = partitionProvider;
        this.amzaRingReader = amzaRingReader;
        this.partitionStateStorage = partitionStateStorage;
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
        partitionStateStorage.streamLocalState((partitionName, ringMember, versionedState) -> {
            long partitionVersion = versionedState.getPartitionVersion();
            VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, partitionVersion);
            if (versionedState.isCurrentState(State.expunged)) {
                compostPartition(composted, versionedPartitionName);
            } else if (!amzaRingReader.isMemberOfRing(partitionName.getRingName())) {
                if (versionedState.isCurrentState(State.bootstrap) || versionedState.isCurrentState(null)) {
                    LOG.info("Composting {} state:{} because we are not a member of the ring",
                        versionedPartitionName, versionedState.getLivelyEndState().getCurrentState());
                    compostPartition(composted, versionedPartitionName);
                } else {
                    LOG.info("Marking {} state:{} for disposal because we are not a member of the ring",
                        versionedPartitionName, versionedState.getLivelyEndState().getCurrentState());
                    partitionStateStorage.markForDisposal(versionedPartitionName, ringMember);
                }
            } else if (!partitionProvider.hasPartition(partitionName)) {
                if (versionedState.isCurrentState(State.bootstrap) || versionedState.isCurrentState(null)) {
                    LOG.info("Composting {} state:{} because no partition is defined on this node",
                        versionedPartitionName, versionedState.getLivelyEndState().getCurrentState());
                    compostPartition(composted, versionedPartitionName);
                } else {
                    LOG.info("Marking {} state:{} for disposal because no partition is defined on this node",
                        versionedPartitionName, versionedState.getLivelyEndState().getCurrentState());
                    partitionStateStorage.markForDisposal(versionedPartitionName, ringMember);
                }
            }
            return true;
        });
        partitionStateStorage.expunged(composted);
    }

    private void compostPartition(List<VersionedPartitionName> composted, VersionedPartitionName versionedPartitionName) {
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        long partitionVersion = versionedPartitionName.getPartitionVersion();
        amzaStats.beginCompaction(CompactionFamily.expunge, versionedPartitionName.toString());
        try {
            LOG.info("Expunging {} {}.", partitionName, partitionVersion);
            partitionStripeProvider.txPartition(partitionName, (PartitionStripe stripe, HighwaterStorage highwaterStorage) -> {
                stripe.deleteDelta(versionedPartitionName);
                partitionIndex.delete(versionedPartitionName);
                highwaterStorage.delete(versionedPartitionName);
                composted.add(versionedPartitionName);
                return null;
            });
            LOG.info("Expunged {} {}.", partitionName, partitionVersion);
        } catch (Exception e) {
            LOG.error("Failed to compost partition {}", new Object[] { versionedPartitionName }, e);
        } finally {
            amzaStats.endCompaction(CompactionFamily.expunge, versionedPartitionName.toString());
        }
    }
}
