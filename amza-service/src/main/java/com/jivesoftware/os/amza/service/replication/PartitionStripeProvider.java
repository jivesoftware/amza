package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;

/**
 * @author jonathan.colt
 */
public class PartitionStripeProvider {

    private final PartitionStripe[] deltaStripes;
    private final HighwaterStorage[] highwaterStorages;

    public PartitionStripeProvider(PartitionStripe[] deltaStripes, HighwaterStorage[] highwaterStorages) {
        this.deltaStripes = deltaStripes;
        this.highwaterStorages = highwaterStorages;
    }

    public <R> R txPartition(PartitionName partitionName, StripeTx<R> tx) throws Exception {
        Preconditions.checkArgument(!partitionName.isSystemPartition(), "No systems allowed.");
        int stripeIndex = Math.abs(partitionName.hashCode()) % deltaStripes.length;
        return tx.tx(deltaStripes[stripeIndex], highwaterStorages[stripeIndex]);
    }

    public void flush(PartitionName partitionName, boolean hardFlush) throws Exception {
        int stripeIndex = Math.abs(partitionName.hashCode()) % deltaStripes.length;
        highwaterStorages[stripeIndex].flush(() -> {
            deltaStripes[stripeIndex].flush(hardFlush);
            return null;
        });
    }

    public void compactAll(boolean force) {
        for (PartitionStripe stripe : deltaStripes) {
            stripe.compact(force);
        }
    }

    public interface StripeTx<R> {

        R tx(PartitionStripe stripe, HighwaterStorage highwaterStorage) throws Exception;
    }

}
