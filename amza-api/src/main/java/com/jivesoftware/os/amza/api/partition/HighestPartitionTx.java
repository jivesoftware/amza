package com.jivesoftware.os.amza.api.partition;

/**
 * @author jonathan.colt
 */
public interface HighestPartitionTx<R> {

    R tx(VersionedAquarium versionedAquarium, long highestTxId) throws Exception;
}
