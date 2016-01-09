package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.api.partition.AquariumTransactor;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;

/**
 *
 */
public class LivelyEndStateTransactor implements AquariumTransactor {

    private final LivelyEndState livelyEndState;

    public LivelyEndStateTransactor(LivelyEndState livelyEndState) {
        this.livelyEndState = livelyEndState;
    }

    @Override
    public LivelyEndState getLivelyEndState(VersionedPartitionName versionedPartitionName) throws Exception {
        return livelyEndState;
    }

    @Override
    public boolean isLivelyEndState(VersionedPartitionName versionedPartitionName, RingMember ringMember) throws Exception {
        return true;
    }

    @Override
    public LivelyEndState awaitOnline(VersionedPartitionName versionedPartitionName, long timeoutMillis) throws Exception {
        return livelyEndState;
    }

    @Override
    public Waterline getLeader(VersionedPartitionName versionedPartitionName) throws Exception {
        return livelyEndState.getLeaderWaterline();
    }

    @Override
    public void wipeTheGlass(VersionedPartitionName versionedPartitionName) throws Exception {
    }

    @Override
    public boolean suggestState(VersionedPartitionName versionedPartitionName, State state) throws Exception {
        return false;
    }

    @Override
    public void tookFully(VersionedPartitionName versionedPartitionName, RingMember fromMember, long leadershipToken) throws Exception {
    }

    @Override
    public boolean isColdstart(VersionedPartitionName versionedPartitionName) throws Exception {
        return false;
    }

    @Override
    public boolean isMemberInState(VersionedPartitionName versionedPartitionName, RingMember ringMember, State state) throws Exception {
        return false;
    }

    @Override
    public void delete(VersionedPartitionName versionedPartitionName) throws Exception {
    }
}
