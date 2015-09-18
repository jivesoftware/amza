package com.jivesoftware.os.amza.api.partition;

import com.jivesoftware.os.aquarium.LivelyEndState;

/**
 * @author jonathan.colt
 */
public interface HighestPartitionTx<R> {

    R tx(VersionedPartitionName versionedPartitionName, LivelyEndState livelyEndState, long highestTxId) throws Exception;
}
