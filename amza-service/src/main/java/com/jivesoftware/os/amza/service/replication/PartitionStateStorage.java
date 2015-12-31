package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionTx;
import com.jivesoftware.os.amza.api.partition.StorageVersion;
import com.jivesoftware.os.amza.api.partition.TxPartitionState;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedState;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.shared.AwaitNotify;
import com.jivesoftware.os.amza.shared.partition.RemoteVersionedState;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionTransactor;
import com.jivesoftware.os.amza.shared.take.CheckState;
import com.jivesoftware.os.amza.shared.take.TakeCoordinator;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class PartitionStateStorage implements TxPartitionState, CheckState {

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
        if (partitionName.isSystemPartition()) {
            return tx.tx(new VersionedPartitionName(partitionName, 0), LivelyEndState.ALWAYS_ONLINE);
        }

        VersionedState versionedState = getLocalVersionedState(partitionName);
        if (versionedState == null) {
            return tx.tx(null, null);
        } else {
            VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, versionedState.getPartitionVersion());
            return transactor.doWithOne(versionedPartitionName, aquariumProvider.getLivelyEndState(versionedPartitionName), tx);
        }
    }

    public void wipeTheGlass(VersionedPartitionName versionedPartitionName) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }

        aquariumProvider.wipeTheGlass(versionedPartitionName, aquariumProvider.getLivelyEndState(versionedPartitionName));
    }

    public long getPartitionVersion(PartitionName partitionName) throws Exception {
        return storageVersionProvider.getPartitionVersion(partitionName);
    }

    @Override
    public VersionedState getLocalVersionedState(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new VersionedState(LivelyEndState.ALWAYS_ONLINE, new StorageVersion(0, 0));
        }

        StorageVersion storageVersion = storageVersionProvider.createIfAbsent(partitionName);
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, storageVersion.partitionVersion);
        return new VersionedState(aquariumProvider.getLivelyEndState(versionedPartitionName), storageVersion);
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

    public VersionedState markAsBootstrap(VersionedPartitionName versionedPartitionName, LivelyEndState livelyEndState) throws Exception {
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        if (partitionName.isSystemPartition()) {
            return new VersionedState(LivelyEndState.ALWAYS_ONLINE, new StorageVersion(0, 0));
        }

        StorageVersion storageVersion = storageVersionProvider.createIfAbsent(partitionName);
        if (storageVersion.partitionVersion == versionedPartitionName.getPartitionVersion()) {
            // let aquarium do its thing
            aquariumProvider.wipeTheGlass(versionedPartitionName, livelyEndState);
        }
        return new VersionedState(livelyEndState, storageVersion);
    }

    public void tookFully(RingMember fromMember, long leadershipToken, VersionedPartitionName versionedPartitionName) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }
        aquariumProvider.tookFully(fromMember, leadershipToken, versionedPartitionName);
    }

    public Waterline getLeader(VersionedPartitionName versionedPartitionName) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return null;
        }
        return aquariumProvider.getAquarium(versionedPartitionName).getLeader();
    }

    public Waterline awaitLeader(PartitionName partitionName, long timeoutMillis) throws Exception {
        if (partitionName.isSystemPartition()) {
            return null;
        }

        if (ringStoreReader.isMemberOfRing(partitionName.getRingName())) {
            StorageVersion storageVersion = storageVersionProvider.createIfAbsent(partitionName);
            VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, storageVersion.partitionVersion);
            Waterline leaderWaterline = aquariumProvider.awaitOnline(versionedPartitionName,
                timeoutMillis).getLeaderWaterline();
            if (!aquariumProvider.isOnline(leaderWaterline)) {
                wipeTheGlass(versionedPartitionName);
            }
            return leaderWaterline;
        } else {
            return aquariumProvider.remoteAwaitProbableLeader(partitionName, timeoutMillis);
        }
    }

    @Override
    public boolean isNominated(RingMember ringMember, VersionedPartitionName versionedPartitionName) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return false;
        }
        return aquariumProvider.getAquarium(versionedPartitionName).isLivelyState(ringMember.asAquariumMember(), State.nominated);
    }

    @Override
    public boolean isOnline(RingMember ringMember, VersionedPartitionName versionedPartitionName) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return true;
        }
        return aquariumProvider.getAquarium(versionedPartitionName).isLivelyEndState(ringMember.asAquariumMember());
    }

    public VersionedState markForDisposal(VersionedPartitionName versionedPartitionName, RingMember ringMember) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return new VersionedState(LivelyEndState.ALWAYS_ONLINE, new StorageVersion(0, 0));
        }

        aquariumProvider.getAquarium(versionedPartitionName).expunge(ringMember.asAquariumMember());

        return getLocalVersionedState(versionedPartitionName.getPartitionName());
    }

    public interface PartitionMemberStateStream {

        boolean stream(PartitionName partitionName, RingMember ringMember, VersionedState versionedState) throws Exception;
    }

    public void streamLocalState(PartitionMemberStateStream stream) throws Exception {
        storageVersionProvider.streamLocal((partitionName, ringMember, storageVersion) -> {
            VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, storageVersion.partitionVersion);
            return transactor.doWithOne(versionedPartitionName, aquariumProvider.getLivelyEndState(versionedPartitionName),
                (localVersionedPartitionName, livelyEndState) -> stream.stream(partitionName, ringMember,
                    new VersionedState(livelyEndState, storageVersion)));
        });
    }

    public void expunged(List<VersionedPartitionName> composted) throws Exception {
        for (VersionedPartitionName compost : composted) {
            LivelyEndState livelyEndState = aquariumProvider.getLivelyEndState(compost);
            if (livelyEndState.getCurrentState() == State.expunged) {
                transactor.doWithAll(compost, livelyEndState,
                    (versionedPartitionName, livelyEndState1) -> {
                        awaitNotify.notifyChange(compost.getPartitionName(), () -> {
                            storageVersionProvider.remove(rootRingMember, compost);
                            return true;
                        });
                        return null;
                    });
            }
        }
        takeCoordinator.expunged(composted);
    }

}
