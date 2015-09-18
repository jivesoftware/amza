package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.aquarium.LivelyEndState;

/**
 * @author jonathan.colt
 */
public interface WALUpdated {

    void updated(VersionedPartitionName versionedPartitionName, LivelyEndState livelyEndState, long txId) throws Exception;

}
