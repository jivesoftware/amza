package com.jivesoftware.os.amza.api.partition;

/**
 * @author jonathan.colt
 */
public interface TxPartitionState {

    <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception;

    VersionedState getLocalVersionedState(PartitionName partitionName) throws Exception;

}
