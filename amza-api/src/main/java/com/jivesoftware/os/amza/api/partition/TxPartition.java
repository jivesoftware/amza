package com.jivesoftware.os.amza.api.partition;

/**
 * @author jonathan.colt
 */
public interface TxPartition {

    <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception;

}
