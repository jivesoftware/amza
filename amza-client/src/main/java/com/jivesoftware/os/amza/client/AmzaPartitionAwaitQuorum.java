package com.jivesoftware.os.amza.client;

import com.jivesoftware.os.amza.shared.AmzaPartitionAPI;
import com.jivesoftware.os.amza.shared.partition.PartitionName;

/**
 *
 * @author jonathan.colt
 */
public interface AmzaPartitionAwaitQuorum {

    void await(PartitionName partitionName, AmzaPartitionAPI.TakeQuorum takeQuorum, int desiredTakeQuorum, long toMillis) throws Exception;

}
