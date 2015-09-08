package com.jivesoftware.os.amza.api.partition;

import com.jivesoftware.os.amza.aquarium.State;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionTx<R> {

    R tx(VersionedPartitionName versionedPartitionName, State state, boolean isOnline) throws Exception;
}
