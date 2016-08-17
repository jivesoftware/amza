package com.jivesoftware.os.amza.service.replication;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionStripeTx<R> {

    R tx(int deltaIndex, int stripeIndex, PartitionStripe partitionStripe) throws Exception;

}
