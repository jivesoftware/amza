package com.jivesoftware.os.amza.api.partition;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionTx<R> {

    R tx(VersionedAquarium versionedAquarium) throws Exception;
}
