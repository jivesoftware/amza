package com.jivesoftware.os.amza.api.partition;

import com.jivesoftware.os.amza.aquarium.Waterline;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionTx<R> {

    R tx(VersionedPartitionName versionedPartitionName, Waterline waterline, boolean isOnline) throws Exception;
}
