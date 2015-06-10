package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;

/**
 *
 * @author jonathan.colt
 */
public interface AmzaPartitionAwaitQuorum {

    /**
     returns how many of the desired were achieved.
     */
    int await(VersionedPartitionName partitionName, AmzaPartitionAPI.TakeQuorum takeQuorum, int desiredTakeQuorum, long toMillis) throws Exception;

}
