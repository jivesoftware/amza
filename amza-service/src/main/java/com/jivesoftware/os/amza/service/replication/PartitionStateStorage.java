package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionTx;
import com.jivesoftware.os.amza.api.partition.StorageVersion;
import com.jivesoftware.os.amza.api.partition.TxPartitionState;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedState;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.shared.AwaitNotify;
import com.jivesoftware.os.amza.shared.partition.RemoteVersionedState;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionTransactor;
import com.jivesoftware.os.amza.shared.take.IsNominated;
import com.jivesoftware.os.amza.shared.take.TakeCoordinator;
import com.jivesoftware.os.aquarium.Aquarium;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class PartitionStateStorage implements TxPartitionState, IsNominated {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RingMember rootRingMember;
    private final AmzaAquariumProvider aquariumProvider;
    private final StorageVersionProvider storageVersionProvider;
    private final TakeCoordinator takeCoordinator;
    private final VersionedPartitionTransactor transactor;
    private final AwaitNotify<PartitionName> awaitNotify;

    public PartitionStateStorage(RingMember rootRingMember,
        AmzaAquariumProvider aquariumProvider,
        StorageVersionProvider storageVersionProvider,
        TakeCoordinator takeCoordinator,
        AwaitNotify<PartitionName> awaitNotify) {
        this.rootRingMember = rootRingMember;
        this.aquariumProvider = aquariumProvider;
        this.storageVersionProvider = storageVersionProvider;
        this.takeCoordinator = takeCoordinator;
        this.transactor = new VersionedPartitionTransactor(1024, 1024); // TODO expose to config?
        this.awaitNotify = awaitNotify;
    }

    @Override
    public <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception {
        if (partitionName.isSystemPartition()) {
            return tx.tx(new VersionedPartitionName(partitionName, 0),
                new Waterline(rootRingMember.asAquariumMember(), State.follower, System.currentTimeMillis(), 0, true, Long.MAX_VALUE), true);
        }

        VersionedState versionedState = getLocalVersionedState(partitionName);
        if (versionedState == null) {
            return tx.tx(null, null, false);
        } else {
            VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, versionedState.storageVersion.partitionVersion);
            return transactor.doWithOne(versionedPartitionName,
                versionedState.waterline,
                aquariumProvider.isOnline(versionedPartitionName, versionedState.waterline),
                tx);
        }
    }

    public void tapTheGlass(VersionedPartitionName versionedPartitionName) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }

        aquariumProvider.getAquarium(versionedPartitionName).tapTheGlass();

        /*ConcurrentHashMap<RingMember, RemoteVersionedState> ringMemberState = remoteStateCache.get(localVersionedPartitionName.getPartitionName());
         if (ringMemberState != null) {
         int inKetchup = 0;
         for (RingMember ringMember : remoteRingMembers) {
         RemoteVersionedState remoteRingMemberState = ringMemberState.get(ringMember);
         if (remoteRingMemberState == null) {
         remoteRingMemberState = getRemoteVersionedState(ringMember, localVersionedPartitionName.getPartitionName());
         }
         if (remoteRingMemberState != null && State.bootstrap == remoteRingMemberState.state) {
         inKetchup++;
         }
         }
         if (inKetchup == remoteRingMembers.size()) {
         markAsOnline(localVersionedPartitionName);
         LOG.info(
         "Resolving cold start stalemate. " + rootRingMember + " was elected as online for " + localVersionedPartitionName
         + " ring size (" + remoteRingMembers.size() + ")");
         }
         }*/
    }

    private Waterline getLocalWaterline(VersionedPartitionName versionedPartitionName) throws Exception {
        Aquarium aquarium = aquariumProvider.getAquarium(versionedPartitionName);
        return aquarium.getState(rootRingMember.asAquariumMember());
    }

    @Override
    public VersionedState getLocalVersionedState(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            Waterline waterline = new Waterline(rootRingMember.asAquariumMember(), State.follower, System.currentTimeMillis(), 0, true, Long.MAX_VALUE);
            return new VersionedState(waterline, true, new StorageVersion(0, 0));
        }

        StorageVersion storageVersion = storageVersionProvider.createIfAbsent(partitionName);
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, storageVersion.partitionVersion);
        Waterline localState = getLocalWaterline(versionedPartitionName);
        return new VersionedState(localState, aquariumProvider.isOnline(versionedPartitionName, localState), storageVersion);
    }

    public RemoteVersionedState getRemoteVersionedState(RingMember remoteRingMember, PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            Waterline waterline = new Waterline(rootRingMember.asAquariumMember(), State.follower, System.currentTimeMillis(), 0, true, Long.MAX_VALUE);
            return new RemoteVersionedState(waterline, 0);
        }

        StorageVersion localStorageVersion = storageVersionProvider.createIfAbsent(partitionName);
        VersionedPartitionName localVersionedPartitionName = new VersionedPartitionName(partitionName, localStorageVersion.partitionVersion);
        Aquarium aquarium = aquariumProvider.getAquarium(localVersionedPartitionName);
        Waterline remoteState = aquarium.getState(remoteRingMember.asAquariumMember());

        StorageVersion remoteStorageVersion = storageVersionProvider.getRemote(remoteRingMember, partitionName);
        return new RemoteVersionedState(remoteState, remoteStorageVersion.partitionVersion);
    }

    public VersionedState markAsBootstrap(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            Waterline waterline = new Waterline(rootRingMember.asAquariumMember(), State.follower, System.currentTimeMillis(), 0, true, Long.MAX_VALUE);
            return new VersionedState(waterline, true, new StorageVersion(0, 0));
        }

        StorageVersion storageVersion = storageVersionProvider.createIfAbsent(partitionName);

        // let aquarium do its thing
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, storageVersion.partitionVersion);
        aquariumProvider.getAquarium(versionedPartitionName).tapTheGlass();
        Waterline localWaterline = getLocalWaterline(versionedPartitionName);

        return new VersionedState(localWaterline, aquariumProvider.isOnline(versionedPartitionName, localWaterline), storageVersion);
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
        StorageVersion storageVersion = storageVersionProvider.createIfAbsent(partitionName);
        return aquariumProvider.getAquarium(new VersionedPartitionName(partitionName, storageVersion.partitionVersion)).awaitLeader(timeoutMillis);
    }

    @Override
    public boolean isNominated(RingMember ringMember, VersionedPartitionName versionedPartitionName) throws Exception {
        return aquariumProvider.getAquarium(versionedPartitionName).isLivelyState(ringMember.asAquariumMember(), State.nominated);
    }

    public VersionedState markForDisposal(VersionedPartitionName versionedPartitionName, RingMember ringMember) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            Waterline waterline = new Waterline(rootRingMember.asAquariumMember(), State.follower, System.currentTimeMillis(), 0, true, Long.MAX_VALUE);
            return new VersionedState(waterline, true, new StorageVersion(0, 0));
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
            Waterline localState = getLocalWaterline(versionedPartitionName);
            return transactor.doWithOne(versionedPartitionName, localState,
                aquariumProvider.isOnline(versionedPartitionName, localState),
                (localVersionedPartitionName, partitionState, isOnline) -> stream.stream(partitionName, ringMember,
                    new VersionedState(partitionState, isOnline, storageVersion)));
        });
    }

    public void expunged(List<VersionedPartitionName> composted) throws Exception {
        for (VersionedPartitionName compost : composted) {
            Waterline waterline = getLocalWaterline(compost);
            if (waterline.getState() == State.expunged) {
                transactor.doWithAll(compost, waterline, aquariumProvider.isOnline(compost, waterline), (versionedPartitionName, partitionState, isOnline) -> {
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
