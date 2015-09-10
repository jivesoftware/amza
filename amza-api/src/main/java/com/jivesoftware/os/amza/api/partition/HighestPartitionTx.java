package com.jivesoftware.os.amza.api.partition;

import com.jivesoftware.os.aquarium.Waterline;

/**
 * @author jonathan.colt
 */
public interface HighestPartitionTx<R> {

    R tx(VersionedPartitionName versionedPartitionName, Waterline waterlineState, boolean isOnline, long highestTxId) throws Exception;
}
