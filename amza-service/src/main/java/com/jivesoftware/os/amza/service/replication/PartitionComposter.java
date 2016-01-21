package com.jivesoftware.os.amza.service.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.stats.AmzaStats.CompactionFamily;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
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
    private final PartitionCreator partitionCreator;
    private final AmzaRingStoreReader amzaRingReader;
    private final PartitionStripeProvider partitionStripeProvider;
    private final PartitionStateStorage partitionStateStorage;
    private final StorageVersionProvider storageVersionProvider;
    private final StripingLocksProvider<PartitionName> stripingLocksProvider;

    public PartitionComposter(AmzaStats amzaStats,
        PartitionIndex partitionIndex,
        PartitionCreator partitionCreator,
        AmzaRingStoreReader amzaRingReader,
        PartitionStateStorage partitionStateStorage,
        PartitionStripeProvider partitionStripeProvider,
        StorageVersionProvider storageVersionProvider) {

        this.amzaStats = amzaStats;
        this.partitionIndex = partitionIndex;
        this.partitionCreator = partitionCreator;
        this.amzaRingReader = amzaRingReader;
        this.partitionStateStorage = partitionStateStorage;
        this.partitionStripeProvider = partitionStripeProvider;
        this.storageVersionProvider = storageVersionProvider;
        this.stripingLocksProvider = new StripingLocksProvider<>(64); //TODO config
    }

    public void start() throws Exception {

        scheduledThreadPool = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("partition-composter-%d").build());
        scheduledThreadPool.scheduleWithFixedDelay(() -> {
            try {
                compostAll();
            } catch (Exception x) {
                LOG.debug("Failing to compact tombstones.", x);

            }
        }, 0, 1, TimeUnit.MINUTES); // TODO config
    }

    public void stop() throws Exception {
        this.scheduledThreadPool.shutdownNow();
        this.scheduledThreadPool = null;
    }

    public void compostAll() throws Exception {
        List<VersionedPartitionName> composted = new ArrayList<>();
        partitionStateStorage.streamLocalAquariums((partitionName, ringMember, versionedAquarium) -> {
            VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
            if (compostIfNecessary(versionedAquarium)) {
                composted.add(versionedPartitionName);
            }
            return true;
        });
        for (VersionedPartitionName compost : composted) {
            partitionStateStorage.expunged(compost);
        }
    }

    public void compostPartitionIfNecessary(PartitionName partitionName) throws Exception {
        VersionedPartitionName compost = partitionStateStorage.tx(partitionName, versionedAquarium -> {
            if (compostIfNecessary(versionedAquarium)) {
                return versionedAquarium.getVersionedPartitionName();
            } else {
                return null;
            }
        });
        if (compost != null) {
            partitionStateStorage.expunged(compost);
        }
    }

    private boolean compostIfNecessary(VersionedAquarium versionedAquarium) throws Exception {
        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        synchronized (stripingLocksProvider.lock(partitionName, 0)) {
            State currentState = versionedAquarium.getLivelyEndState().getCurrentState();
            if (currentState == State.expunged
                || !storageVersionProvider.isCurrentVersion(versionedPartitionName)
                || partitionCreator.isPartitionDisposed(partitionName)) {
                deletePartition(versionedPartitionName);
                return true;
            } else {
                if (!amzaRingReader.isMemberOfRing(partitionName.getRingName())) {
                    if (currentState == State.bootstrap || currentState == null) {
                        LOG.info("Composting {} state:{} because we are not a member of the ring",
                            versionedPartitionName, currentState);
                        deletePartition(versionedPartitionName);
                        return true;
                    } else {
                        LOG.info("Marking {} state:{} for disposal because we are not a member of the ring",
                            versionedPartitionName, currentState);
                        versionedAquarium.suggestState(State.expunged);
                        return false;
                    }
                } else if (!partitionCreator.hasPartition(partitionName)) {
                    if (currentState == State.bootstrap || currentState == null) {
                        LOG.info("Composting {} state:{} because no partition is defined on this node",
                            versionedPartitionName, currentState);
                        deletePartition(versionedPartitionName);
                        return true;
                    } else {
                        LOG.info("Marking {} state:{} for disposal because no partition is defined on this node",
                            versionedPartitionName, currentState);
                        versionedAquarium.suggestState(State.expunged);
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
    }

    private void deletePartition(VersionedPartitionName versionedPartitionName) {
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        long partitionVersion = versionedPartitionName.getPartitionVersion();
        amzaStats.beginCompaction(CompactionFamily.expunge, versionedPartitionName.toString());
        try {
            LOG.info("Expunging {} {}.", partitionName, partitionVersion);
            partitionStripeProvider.txPartition(partitionName, (PartitionStripe stripe, HighwaterStorage highwaterStorage) -> {
                stripe.deleteDelta(versionedPartitionName);
                partitionIndex.delete(versionedPartitionName);
                highwaterStorage.delete(versionedPartitionName);
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
