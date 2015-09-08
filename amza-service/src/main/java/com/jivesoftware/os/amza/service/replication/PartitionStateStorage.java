package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionTx;
import com.jivesoftware.os.amza.api.partition.StorageVersion;
import com.jivesoftware.os.amza.api.partition.TxPartitionState;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedState;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.aquarium.Aquarium;
import com.jivesoftware.os.amza.aquarium.State;
import com.jivesoftware.os.amza.shared.AwaitNotify;
import com.jivesoftware.os.amza.shared.partition.RemoteVersionedState;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionTransactor;
import com.jivesoftware.os.amza.shared.take.TakeCoordinator;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jonathan.colt
 */
public class PartitionStateStorage implements TxPartitionState {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider orderIdProvider;
    private final RingMember rootRingMember;
    private final AmzaAquariumProvider aquariumProvider;
    private final StorageVersionProvider storageVersionProvider;
    private final TakeCoordinator takeCoordinator;
    private final VersionedPartitionTransactor transactor;
    private final AwaitNotify<PartitionName> awaitNotify;

    private final ConcurrentHashMap<VersionedPartitionName, Aquarium> aquariums = new ConcurrentHashMap<>();

    public PartitionStateStorage(OrderIdProvider orderIdProvider,
        RingMember rootRingMember,
        AmzaAquariumProvider aquariumProvider,
        StorageVersionProvider storageVersionProvider,
        TakeCoordinator takeCoordinator,
        AwaitNotify<PartitionName> awaitNotify) {
        this.orderIdProvider = orderIdProvider;
        this.rootRingMember = rootRingMember;
        this.aquariumProvider = aquariumProvider;
        this.storageVersionProvider = storageVersionProvider;
        this.takeCoordinator = takeCoordinator;
        this.transactor = new VersionedPartitionTransactor(1024, 1024); // TODO expose to config?
        this.awaitNotify = awaitNotify;
    }

    private Aquarium getAquarium(VersionedPartitionName versionedPartitionName) throws Exception {
        return aquariums.computeIfAbsent(versionedPartitionName, key -> {
            try {
                return aquariumProvider.getAquarium(key);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get aquarium for partition " + versionedPartitionName, e);
            }
        });
    }

    @Override
    public <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception {
        if (partitionName.isSystemPartition()) {
            return tx.tx(new VersionedPartitionName(partitionName, 0), State.follower, true);
        }

        VersionedState versionedState = getLocalVersionedState(partitionName);
        if (versionedState == null) {
            return tx.tx(null, null, false);
        } else {
            VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, versionedState.storageVersion.partitionVersion);
            return transactor.doWithOne(versionedPartitionName, versionedState.state, isOnline(versionedPartitionName, versionedState.state), tx);
        }
    }

    public void tapTheGlass(VersionedPartitionName versionedPartitionName) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }

        getAquarium(versionedPartitionName).tapTheGlass();

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

    private State getLocalState(VersionedPartitionName versionedPartitionName) throws Exception {
        Aquarium aquarium = getAquarium(versionedPartitionName);
        return aquarium.getState(rootRingMember.asAquariumMember());
    }

    @Override
    public VersionedState getLocalVersionedState(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new VersionedState(State.follower, true, new StorageVersion(0, 0));
        }

        StorageVersion storageVersion = storageVersionProvider.get(partitionName);
        if (storageVersion == null) {
            return null;
        }
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, storageVersion.partitionVersion);
        State localState = getLocalState(versionedPartitionName);
        return new VersionedState(localState, isOnline(versionedPartitionName, localState), storageVersion);
    }

    public RemoteVersionedState getRemoteVersionedState(RingMember ringMember, PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new RemoteVersionedState(State.follower, 0);
        }

        StorageVersion storageVersion = storageVersionProvider.getRemote(ringMember, partitionName);
        VersionedPartitionName remoteVersionedPartitionName = new VersionedPartitionName(partitionName, storageVersion.partitionVersion);
        Aquarium aquarium = aquariumProvider.getAquarium(remoteVersionedPartitionName);
        return new RemoteVersionedState(aquarium.getState(ringMember.asAquariumMember()), storageVersion.partitionVersion);
    }

    public VersionedState markAsBootstrap(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new VersionedState(State.follower, true, new StorageVersion(0, 0));
        }

        long partitionVersion = orderIdProvider.nextId();
        StorageVersion storageVersion = storageVersionProvider.set(rootRingMember, partitionName, partitionVersion);

        // let aquarium do its thing
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, storageVersion.partitionVersion);
        getAquarium(versionedPartitionName).tapTheGlass();
        State localState = getLocalState(versionedPartitionName);

        return new VersionedState(localState, isOnline(versionedPartitionName, localState), storageVersion);
    }

    public void tookFully(VersionedPartitionName versionedPartitionName, RingMember fromMember) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }

        aquariumProvider.tookFully(fromMember, versionedPartitionName);

        // let aquarium do its thing
        getAquarium(versionedPartitionName).tapTheGlass();
    }

    public void markAsOnline(VersionedPartitionName versionedPartitionName) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }

        // let aquarium do its thing
        getAquarium(versionedPartitionName).tapTheGlass();
    }

    public VersionedState markForDisposal(VersionedPartitionName versionedPartitionName, RingMember ringMember) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return new VersionedState(State.follower, true, new StorageVersion(0, 0));
        }

        getAquarium(versionedPartitionName).expunge(ringMember.asAquariumMember());

        StorageVersion storageVersion = storageVersionProvider.set(ringMember, versionedPartitionName.getPartitionName(),
            versionedPartitionName.getPartitionVersion());
        State localState = getLocalState(versionedPartitionName);
        return new VersionedState(localState, isOnline(versionedPartitionName, localState), storageVersion);
    }

    public boolean isOnline(VersionedPartitionName versionedPartitionName,
        State partitionState) throws Exception {
        if (partitionState == State.follower || partitionState == State.leader) {
            State livelyEndState = getAquarium(versionedPartitionName).livelyEndState();
            return livelyEndState == State.follower || livelyEndState == State.leader;
        } else {
            return false;
        }
    }

    public interface PartitionMemberStateStream {

        boolean stream(PartitionName partitionName, RingMember ringMember, VersionedState versionedState) throws Exception;
    }

    public void streamLocalState(PartitionMemberStateStream stream) throws Exception {
        storageVersionProvider.streamLocal((partitionName, ringMember, storageVersion) -> {
            VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, storageVersion.partitionVersion);
            State localState = getLocalState(versionedPartitionName);
            return transactor.doWithOne(versionedPartitionName, localState,
                isOnline(versionedPartitionName, localState),
                (localVersionedPartitionName, partitionState, isOnline) -> stream.stream(partitionName, ringMember,
                    new VersionedState(partitionState, isOnline, storageVersion)));
        });
    }

    public void expunged(List<VersionedPartitionName> composted) throws Exception {
        for (VersionedPartitionName compost : composted) {
            State state = getLocalState(compost);
            if (state == State.expunged) {
                transactor.doWithAll(compost, state, isOnline(compost, state), (versionedPartitionName, partitionState, isOnline) -> {
                    awaitNotify.notifyChange(compost.getPartitionName(), () -> storageVersionProvider.remove(rootRingMember, compost));
                    return null;
                });
            }
        }
        takeCoordinator.expunged(composted);
    }

    public void awaitOnline(PartitionName partitionName, long timeoutMillis) throws Exception {
        awaitNotify.awaitChange(partitionName, () -> {
            VersionedState versionedState = getLocalVersionedState(partitionName);
            if (versionedState != null) {
                if (versionedState.state == State.expunged) {
                    throw new IllegalStateException("Partition is being expunged");
                } else if (isOnline(new VersionedPartitionName(partitionName, versionedState.storageVersion.partitionVersion), versionedState.state)) {
                    return Optional.absent();
                }
            }
            return null;
        }, timeoutMillis);
    }

}
