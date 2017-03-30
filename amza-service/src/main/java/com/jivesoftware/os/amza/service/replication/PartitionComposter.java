package com.jivesoftware.os.amza.service.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.NotARingMemberException;
import com.jivesoftware.os.amza.service.PartitionIsDisposedException;
import com.jivesoftware.os.amza.service.StripingLocksProvider;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.stats.AmzaStats.CompactionFamily;
import com.jivesoftware.os.amza.service.stats.AmzaStats.CompactionStats;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.jive.utils.collections.bah.ConcurrentBAHash;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.jivesoftware.os.amza.service.storage.PartitionCreator.AQUARIUM_STATE_INDEX;
import static com.jivesoftware.os.amza.service.storage.PartitionCreator.PARTITION_VERSION_INDEX;
import static com.jivesoftware.os.amza.service.storage.PartitionCreator.REGION_INDEX;

/**
 * @author jonathan.colt
 */
public class PartitionComposter implements RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private ScheduledExecutorService scheduledThreadPool;

    private final AmzaStats amzaStats;
    private final PartitionIndex partitionIndex;
    private final PartitionCreator partitionCreator;
    private final AmzaRingStoreReader amzaRingReader;
    private final PartitionStripeProvider partitionStripeProvider;
    private final StorageVersionProvider storageVersionProvider;
    private final AmzaInterner amzaInterner;
    private final StripingLocksProvider<PartitionName> stripingLocksProvider;
    private final ConcurrentBAHash<byte[]> dirtyPartitions;

    private volatile boolean coldstart = true;

    public PartitionComposter(AmzaStats amzaStats,
        PartitionIndex partitionIndex,
        PartitionCreator partitionCreator,
        AmzaRingStoreReader amzaRingReader,
        PartitionStripeProvider partitionStripeProvider,
        StorageVersionProvider storageVersionProvider,
        AmzaInterner amzaInterner,
        int concurrency) {

        this.amzaStats = amzaStats;
        this.partitionIndex = partitionIndex;
        this.partitionCreator = partitionCreator;
        this.amzaRingReader = amzaRingReader;
        this.partitionStripeProvider = partitionStripeProvider;
        this.storageVersionProvider = storageVersionProvider;
        this.amzaInterner = amzaInterner;
        this.stripingLocksProvider = new StripingLocksProvider<>(64); //TODO config
        this.dirtyPartitions = new ConcurrentBAHash<>(3, false, concurrency);
    }

    public void start() throws Exception {

        scheduledThreadPool = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("partition-composter-%d").build());
        scheduledThreadPool.scheduleWithFixedDelay(() -> {
            try {
                compostAll();
            } catch (Exception x) {
                LOG.error("Failing to compost", x);
            }
        }, 0, 1, TimeUnit.MINUTES); // TODO config
    }

    public void stop() throws Exception {
        this.scheduledThreadPool.shutdownNow();
        this.scheduledThreadPool = null;
    }

    @Override
    public void changes(RowsChanged changes) throws Exception {
        PartitionName partitionName = changes.getVersionedPartitionName().getPartitionName();
        if (partitionName.equals(REGION_INDEX.getPartitionName())) {
            for (WALKey key : changes.getApply().keySet()) {
                dirtyPartitions.put(key.key, key.key);
            }
        } else if (partitionName.equals(PARTITION_VERSION_INDEX.getPartitionName())) {
            for (WALKey key : changes.getApply().keySet()) {
                byte[] dirtyBytes = storageVersionProvider.partitionNameFromKey(key.key).toBytes();
                dirtyPartitions.put(dirtyBytes, dirtyBytes);
            }
        } else if (partitionName.equals(AQUARIUM_STATE_INDEX.getPartitionName())) {
            for (WALKey key : changes.getApply().keySet()) {
                AmzaAquariumProvider.streamStateKey(key.key, amzaInterner,
                    (dirtyPartitionName, context, rootRingMember, partitionVersion, isSelf, ackRingMember) -> {
                        byte[] dirtyBytes = dirtyPartitionName.toBytes();
                        dirtyPartitions.put(dirtyBytes, dirtyBytes);
                        return true;
                    });
            }
        }
    }

    public void compostAll() throws Exception {
        List<VersionedPartitionName> composted = new ArrayList<>();
        try {
            if (coldstart) {
                partitionStripeProvider.streamLocalAquariums((partitionName, ringMember, versionedAquarium) -> {
                    VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
                    if (compostIfNecessary(versionedAquarium)) {
                        composted.add(versionedPartitionName);
                    }
                    return true;
                });
                coldstart = false;
            } else {
                dirtyPartitions.stream((key, value) -> {
                    PartitionName partitionName = amzaInterner.internPartitionName(key, 0, key.length);
                    dirtyPartitions.remove(key);
                    try {
                        partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
                            VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
                            if (compostIfNecessary(versionedAquarium)) {
                                composted.add(versionedPartitionName);
                            }
                            return null;
                        });
                        return true;
                    } catch (PartitionIsDisposedException e) {
                        LOG.info("Ignored disposed partition: {}", partitionName);
                        return true;
                    } catch (NotARingMemberException e) {
                        LOG.debug("Skipped compost for non-member partition without storage: {}", partitionName);
                        return true;
                    } catch (Throwable t) {
                        dirtyPartitions.put(key, key);
                        throw t;
                    }
                });
            }
        } catch (Exception e) {
            LOG.warn("Error while composting partitions", e);
        }

        for (VersionedPartitionName compost : composted) {
            try {
                partitionStripeProvider.expunged(compost);
            } catch (Exception e) {
                LOG.warn("Failed to expunge {}", new Object[]{compost}, e);
            }
        }
    }

    public void compostPartitionIfNecessary(PartitionName partitionName) throws Exception {
        VersionedPartitionName compost = partitionStripeProvider.txPartition(partitionName,
            (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
                if (compostIfNecessary(versionedAquarium)) {
                    return versionedAquarium.getVersionedPartitionName();
                } else {
                    return null;
                }
            });
        if (compost != null) {
            partitionStripeProvider.expunged(compost);
        }
    }

    private boolean compostIfNecessary(VersionedAquarium versionedAquarium) throws Exception {
        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        synchronized (stripingLocksProvider.lock(partitionName, 0)) {
            State currentState = versionedAquarium.getLivelyEndState().getCurrentState();
            if (currentState == State.expunged // Aquarium State
                || !storageVersionProvider.isCurrentVersion(versionedPartitionName)) { // Partition Version
                deletePartition(versionedPartitionName);
                return true;
            } else if (!amzaRingReader.isMemberOfRing(partitionName.getRingName(), 0)) {
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

    private void deletePartition(VersionedPartitionName versionedPartitionName) {
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        long partitionVersion = versionedPartitionName.getPartitionVersion();
        CompactionStats compactionStats = amzaStats.beginCompaction(CompactionFamily.expunge, versionedPartitionName.toString());
        try {
            LOG.info("Expunging {} {}.", partitionName, partitionVersion);
            partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
                txPartitionStripe.tx((deltaIndex, stripeIndex, partitionStripe) -> {
                    partitionStripe.deleteDelta(versionedPartitionName);
                    partitionIndex.delete(versionedPartitionName, stripeIndex);
                    return null;
                });
                highwaterStorage.delete(versionedPartitionName);
                return null;
            });
            LOG.info("Expunged {} {}.", partitionName, partitionVersion);
        } catch (Exception e) {
            LOG.error("Failed to compost partition {}", new Object[]{versionedPartitionName}, e);
        } finally {
            compactionStats.finished();
        }
    }
}
