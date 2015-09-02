package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.aquarium.State;

/**
 * @author jonathan.colt
 */
public interface WALUpdated {

    void updated(VersionedPartitionName versionedPartitionName, State state, long txId) throws Exception;

}
