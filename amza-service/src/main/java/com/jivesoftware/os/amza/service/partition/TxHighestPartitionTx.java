package com.jivesoftware.os.amza.service.partition;

import com.jivesoftware.os.amza.api.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.api.partition.PartitionName;

/**
 *
 * @author jonathan.colt
 */
public interface TxHighestPartitionTx<R> {

    R tx(PartitionName partitionName, HighestPartitionTx<R> highestPartitionTx) throws Exception;
}
