package com.jivesoftware.os.amza.api.partition;

import com.jivesoftware.os.aquarium.LivelyEndState;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionTx<R> {

    R tx(VersionedPartitionName versionedPartitionName, LivelyEndState livelyEndState) throws Exception;
}
