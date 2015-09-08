package com.jivesoftware.os.amza.api.partition;

import com.jivesoftware.os.amza.aquarium.State;

/**
 * @author jonathan.colt
 */
public interface HighestPartitionTx<R> {

    R tx(VersionedPartitionName versionedPartitionName, State partitionState, boolean isOnline, long highestTxId) throws Exception;
}
