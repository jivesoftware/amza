package com.jivesoftware.os.amza.api.partition;

/**
 * @author jonathan.colt
 */
public interface HighestPartitionTx {

    long tx(VersionedAquarium versionedAquarium, long highestTxId) throws Exception;
}
