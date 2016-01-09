package com.jivesoftware.os.amza.api.partition;

import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;

/**
 *
 */
public interface AquariumTransactor {

    LivelyEndState getLivelyEndState(VersionedPartitionName versionedPartitionName) throws Exception;

    boolean isLivelyEndState(VersionedPartitionName versionedPartitionName, RingMember ringMember) throws Exception;

    LivelyEndState awaitOnline(VersionedPartitionName versionedPartitionName, long timeoutMillis) throws Exception;

    Waterline getLeader(VersionedPartitionName versionedPartitionName) throws Exception;

    void wipeTheGlass(VersionedPartitionName versionedPartitionName) throws Exception;

    boolean suggestState(VersionedPartitionName versionedPartitionName, State state) throws Exception;

    void tookFully(VersionedPartitionName versionedPartitionName, RingMember fromMember, long leadershipToken) throws Exception;

    boolean isColdstart(VersionedPartitionName versionedPartitionName) throws Exception;

    boolean isMemberInState(VersionedPartitionName versionedPartitionName, RingMember ringMember, State state) throws Exception;

    void delete(VersionedPartitionName versionedPartitionName) throws Exception;
}
