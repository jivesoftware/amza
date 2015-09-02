package com.jivesoftware.os.amza.api.partition;

import com.jivesoftware.os.amza.aquarium.State;

/**
 * @author jonathan.colt
 */
public interface HighestPartitionTx {

    void tx(VersionedPartitionName versionedPartitionName, State partitionState, long highestTxId) throws Exception;
}
