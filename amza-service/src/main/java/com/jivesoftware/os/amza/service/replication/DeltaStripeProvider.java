package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author jonathan.colt
 */
public class DeltaStripeProvider {

    private final Random rand = new Random();
    private final PartitionStripe[] partitionStripes;
    private final ConcurrentHashMap<PartitionName, PartitionStripe> cache = new ConcurrentHashMap<>();

    public DeltaStripeProvider(PartitionStripe[] partitionStripes) {
        this.partitionStripes = partitionStripes;
    }

    public void register(PartitionName partitionName, PartitionStripe stripe) {

    }

    public PartitionStripe get(PartitionName partitionName) {
        return cache.computeIfAbsent(partitionName, (t) -> partitionStripes[rand.nextInt(partitionStripes.length)]);
    }

    public void release(PartitionName partitionName, PartitionStripe stripe) {

    }
}
