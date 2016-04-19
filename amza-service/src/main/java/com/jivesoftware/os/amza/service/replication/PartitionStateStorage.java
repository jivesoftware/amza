package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionTx;
import com.jivesoftware.os.amza.api.partition.RemoteVersionedState;
import com.jivesoftware.os.amza.api.partition.StorageVersion;
import com.jivesoftware.os.amza.api.partition.TxPartitionState;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.AwaitNotify;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionTransactor;
import com.jivesoftware.os.amza.service.take.TakeCoordinator;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 * @author jonathan.colt
 */
public class PartitionStateStorage implements TxPartitionState {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RingMember rootRingMember;
    private final AmzaRingStoreReader ringStoreReader;
    private final AmzaAquariumProvider aquariumProvider;
    private final StorageVersionProvider storageVersionProvider;
    private final TakeCoordinator takeCoordinator;
    private final VersionedPartitionTransactor transactor;
    private final AwaitNotify<PartitionName> awaitNotify;

    public PartitionStateStorage(RingMember rootRingMember,
        AmzaRingStoreReader ringStoreReader,
        AmzaAquariumProvider aquariumProvider,
        StorageVersionProvider storageVersionProvider,
        TakeCoordinator takeCoordinator,
        AwaitNotify<PartitionName> awaitNotify) {
        this.rootRingMember = rootRingMember;
        this.ringStoreReader = ringStoreReader;
        this.aquariumProvider = aquariumProvider;
        this.storageVersionProvider = storageVersionProvider;
        this.takeCoordinator = takeCoordinator;
        this.transactor = new VersionedPartitionTransactor(1024, 1024); // TODO expose to config?
        this.awaitNotify = awaitNotify;
    }

    @Override
    public <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception {

        VersionedAquarium versionedAquarium;
        int stripe;
        if (partitionName.isSystemPartition()) {
            versionedAquarium = new VersionedAquarium(new VersionedPartitionName(partitionName, 0), null, 0);
            stripe = storageVersionProvider.getSystemStripe(partitionName);
        } else {
            StorageVersion storageVersion = storageVersionProvider.createIfAbsent(partitionName);
            stripe = storageVersionProvider.getCurrentStripe(storageVersion);
            VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, storageVersion.partitionVersion);
            versionedAquarium = new VersionedAquarium(versionedPartitionName, aquariumProvider, storageVersion.stripeVersion);
        }
        if (stripe == -1) {
            throw new IllegalStateException("Failed to compute stripe for " + partitionName);
        }
        return transactor.doWithOne(versionedAquarium, stripe, tx);
    }

    public RemoteVersionedState getRemoteVersionedState(RingMember remoteRingMember, PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new RemoteVersionedState(Waterline.ALWAYS_ONLINE, 0);
        }

        StorageVersion remoteStorageVersion = storageVersionProvider.getRemote(remoteRingMember, partitionName);
        if (remoteStorageVersion == null) {
            return null;
        }

        Waterline remoteState = aquariumProvider.getCurrentState(partitionName, remoteRingMember, remoteStorageVersion.partitionVersion);
        return new RemoteVersionedState(remoteState, remoteStorageVersion.partitionVersion);
    }

    public Waterline awaitLeader(PartitionName partitionName, long timeoutMillis) throws Exception {
        if (partitionName.isSystemPartition()) {
            return null;
        }

        if (ringStoreReader.isMemberOfRing(partitionName.getRingName())) {
            return tx(partitionName, (versionedAquarium, stripe) -> {
                Waterline leaderWaterline = versionedAquarium.awaitOnline(timeoutMillis).getLeaderWaterline();
                if (!aquariumProvider.isOnline(leaderWaterline)) {
                    versionedAquarium.wipeTheGlass();
                }
                return leaderWaterline;
            });
        } else {
            return aquariumProvider.remoteAwaitProbableLeader(partitionName, timeoutMillis);
        }
    }

    public interface PartitionMemberStateStream {

        boolean stream(PartitionName partitionName, RingMember ringMember, VersionedAquarium versionedAquarium, int stripe) throws Exception;
    }

    public void streamLocalAquariums(PartitionMemberStateStream stream) throws Exception {
        storageVersionProvider.streamLocal((partitionName, ringMember, storageVersion) -> {
            VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, storageVersion.partitionVersion);
            VersionedAquarium versionedAquarium = new VersionedAquarium(versionedPartitionName, aquariumProvider, storageVersion.stripeVersion);
            int stripe = stripe(partitionName, storageVersion);
            return transactor.doWithOne(versionedAquarium, stripe,
                (versionedAquarium1, stripe1) -> stream.stream(partitionName, ringMember, versionedAquarium1, stripe1));
        });
    }

    public void expunged(VersionedPartitionName versionedPartitionName) throws Exception {
        VersionedAquarium versionedAquarium = new VersionedAquarium(versionedPartitionName, aquariumProvider, -1);
        StorageVersion storageVersion = storageVersionProvider.lookupStorageVersion(versionedPartitionName.getPartitionName());
        int stripe = storageVersion == null ? -1 : storageVersionProvider.getCurrentStripe(storageVersion);

        if (stripe != -1) {
            LOG.info("Removing storage versions for composted partition: {} stripe:{}", versionedPartitionName, stripe);
            transactor.doWithAll(versionedAquarium, stripe, (versionedAquarium1, stripe1) -> {
                awaitNotify.notifyChange(versionedPartitionName.getPartitionName(), () -> {
                    versionedAquarium1.delete();
                    storageVersionProvider.remove(rootRingMember, versionedAquarium1.getVersionedPartitionName());
                    return true;
                });
                return null;
            });
            takeCoordinator.expunged(versionedPartitionName);
        } else {
            LOG.warn("Failed to locate storage versions for composted partition: {}", versionedPartitionName);
        }
    }

    private int stripe(PartitionName partitionName, StorageVersion storageVersion) {
        int stripe = -1;
        if (partitionName.isSystemPartition()) {
            stripe = storageVersionProvider.getSystemStripe(partitionName);
        } else {
            stripe = storageVersionProvider.getCurrentStripe(storageVersion);
        }
        return stripe;
    }

}
