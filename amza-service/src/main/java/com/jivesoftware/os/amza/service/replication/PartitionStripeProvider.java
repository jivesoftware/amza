package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.amza.service.take.RowsTaker;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

/**
 * @author jonathan.colt
 */
public class PartitionStripeProvider {

    private final PartitionStripeFunction partitionStripeFunction;
    private final PartitionStripe[] deltaStripes;
    private final HighwaterStorage[] highwaterStorages;
    private final ExecutorService[] rowTakerThreadPools;
    private final RowsTaker[] rowsTakers;

    public PartitionStripeProvider(PartitionStripeFunction partitionStripeFunction,
        PartitionStripe[] deltaStripes,
        HighwaterStorage[] highwaterStorages,
        ExecutorService[] rowTakerThreadPools,
        RowsTaker[] rowsTakers) {
        this.partitionStripeFunction = partitionStripeFunction;
        this.deltaStripes = Arrays.copyOf(deltaStripes, deltaStripes.length);
        this.highwaterStorages = Arrays.copyOf(highwaterStorages, highwaterStorages.length);
        this.rowTakerThreadPools = Arrays.copyOf(rowTakerThreadPools, rowTakerThreadPools.length);
        this.rowsTakers = Arrays.copyOf(rowsTakers, rowsTakers.length);
    }

    public <R> R txPartition(PartitionName partitionName, StripeTx<R> tx) throws Exception {
        Preconditions.checkArgument(!partitionName.isSystemPartition(), "No systems allowed.");
        int stripeIndex = partitionStripeFunction.stripe(partitionName);
        return tx.tx(deltaStripes[stripeIndex], highwaterStorages[stripeIndex]);
    }
    
    public void flush(PartitionName partitionName, boolean hardFlush) throws Exception {
        int stripeIndex = partitionStripeFunction.stripe(partitionName);
        highwaterStorages[stripeIndex].flush(() -> {
            deltaStripes[stripeIndex].flush(hardFlush);
            return null;
        });
    }

    ExecutorService getRowTakerThreadPool(PartitionName partitionName) {
        int stripeIndex = partitionStripeFunction.stripe(partitionName);
        return rowTakerThreadPools[stripeIndex];
    }

    RowsTaker getRowsTaker(PartitionName partitionName) {
        int stripeIndex = partitionStripeFunction.stripe(partitionName);
        return rowsTakers[stripeIndex];
    }

    public void mergeAll(boolean force) {
        for (PartitionStripe stripe : deltaStripes) {
            stripe.merge(force);
        }
    }

    public void start() {
    }

    public void stop() {
        for (ExecutorService rowTakerThreadPool : rowTakerThreadPools) {
            rowTakerThreadPool.shutdownNow();
        }
    }

    public interface StripeTx<R> {

        R tx(PartitionStripe stripe, HighwaterStorage highwaterStorage) throws Exception;
    }

}
