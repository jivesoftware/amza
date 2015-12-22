package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.partition.PartitionName;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionClientFactory<C, E extends Throwable> {

    PartitionClient create(PartitionName partitionName,
        AmzaClientCallRouter<C, E> partitionCallRouter,
        long awaitLeaderElectionForNMillis) throws Exception;
}
