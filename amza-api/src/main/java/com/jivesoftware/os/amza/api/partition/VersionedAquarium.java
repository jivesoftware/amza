package com.jivesoftware.os.amza.api.partition;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;

/**
 * @author jonathan.colt
 */
public class VersionedAquarium {

    private final VersionedPartitionName versionedPartitionName;
    private final AquariumTransactor aquariumTransactor;

    public VersionedAquarium(VersionedPartitionName versionedPartitionName,
        AquariumTransactor aquariumTransactor) {
        this.versionedPartitionName = Preconditions.checkNotNull(versionedPartitionName, "VersionedPartitionName cannot be null");
        Preconditions.checkArgument(versionedPartitionName.getPartitionName().isSystemPartition() || aquariumTransactor != null,
            "Non system partitions require an AquariumTransactor");
        this.aquariumTransactor = aquariumTransactor;
    }

    public VersionedPartitionName getVersionedPartitionName() {
        return versionedPartitionName;
    }

    public LivelyEndState getLivelyEndState() throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return LivelyEndState.ALWAYS_ONLINE;
        }
        return aquariumTransactor.getLivelyEndState(versionedPartitionName);
    }

    public boolean isLivelyEndState(RingMember ringMember) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return true;
        }
        return aquariumTransactor.isLivelyEndState(versionedPartitionName, ringMember);
    }

    public LivelyEndState awaitOnline(long timeoutMillis) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return LivelyEndState.ALWAYS_ONLINE;
        }
        return aquariumTransactor.awaitOnline(versionedPartitionName, timeoutMillis);
    }

    public Waterline getLeader() throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return null;
        }
        return aquariumTransactor.getLeader(versionedPartitionName);
    }

    public void wipeTheGlass() throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }
        aquariumTransactor.wipeTheGlass(versionedPartitionName);
    }

    public boolean suggestState(State state) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return false;
        }
        return aquariumTransactor.suggestState(versionedPartitionName, state);
    }

    public void tookFully(RingMember fromMember, long leadershipToken) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }
        aquariumTransactor.tookFully(versionedPartitionName, fromMember, leadershipToken);
    }

    public boolean isColdstart() throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return false;
        }
        return aquariumTransactor.isColdstart(versionedPartitionName);
    }

    public boolean isMemberInState(RingMember ringMember, State state) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return state == State.follower;
        }
        return aquariumTransactor.isMemberInState(versionedPartitionName, ringMember, state);
    }

    public void delete() throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }
        aquariumTransactor.delete(versionedPartitionName);
    }

    @Override
    public boolean equals(Object obj) {
        throw new UnsupportedOperationException("Stop that");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Stop that");
    }

    @Override
    public String toString() {
        return "VersionedState{" + "versionedPartitionName=" + versionedPartitionName + '}';
    }

}
