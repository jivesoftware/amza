package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.WALIndex;
import com.jivesoftware.os.amza.api.wal.WALIndexProvider;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import java.io.File;
import java.util.Random;
import org.apache.commons.io.FileUtils;

/**
 * @author jonathan.colt
 */
public class IndexedWALStorageProvider {

    private final Random rand = new Random();
    private final AmzaStats amzaStats;
    private final File[] workingDirectories;
    private final int numberOfStripes;
    private final WALIndexProviderRegistry indexProviderRegistry;
    private final PrimaryRowMarshaller primaryRowMarshaller;
    private final BinaryHighwaterRowMarshaller highwaterRowMarshaller;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final SickPartitions sickPartitions;
    private final int tombstoneCompactionFactor;
    private final long rebalanceIfImbalanceGreaterThanInBytes;

    public IndexedWALStorageProvider(AmzaStats amzaStats,
        File[] workingDirectories,
        int numberOfStripes,
        WALIndexProviderRegistry indexProviderRegistry,
        PrimaryRowMarshaller primaryRowMarshaller,
        BinaryHighwaterRowMarshaller highwaterRowMarshaller,
        TimestampedOrderIdProvider orderIdProvider,
        SickPartitions sickPartitions,
        int tombstoneCompactionFactor,
        long rebalanceIfImbalanceGreaterThanInBytes) {
        this.amzaStats = amzaStats;

        this.workingDirectories = workingDirectories;
        this.numberOfStripes = numberOfStripes;
        this.indexProviderRegistry = indexProviderRegistry;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.orderIdProvider = orderIdProvider;
        this.sickPartitions = sickPartitions;
        this.tombstoneCompactionFactor = tombstoneCompactionFactor;
        this.rebalanceIfImbalanceGreaterThanInBytes = rebalanceIfImbalanceGreaterThanInBytes;
    }

    public int rebalanceToStripe(VersionedPartitionName versionedPartitionName, int stripe) {
        int numberOfWorkingDirectories = workingDirectories.length;
        long[] freeSpace = new long[numberOfWorkingDirectories];
        long maxFree = Long.MIN_VALUE;
        int maxFreeIndex = -1;

        long minFree = Long.MAX_VALUE;
        int minFreeIndex = -1;
        for (int i = 0; i < numberOfWorkingDirectories; i++) {
            freeSpace[i] = workingDirectories[i].getFreeSpace();
            if (freeSpace[i] < minFree) {
                minFree = freeSpace[i];
                minFreeIndex = i;
            }
            if (freeSpace[i] > maxFree) {
                maxFree = freeSpace[i];
                maxFreeIndex = i;
            }
        }

        long imbalance = freeSpace[maxFreeIndex] - freeSpace[minFreeIndex];

        if (imbalance > rebalanceIfImbalanceGreaterThanInBytes) {
            int maxStripeCount = (numberOfStripes / numberOfWorkingDirectories) + (maxFreeIndex < (numberOfStripes % numberOfWorkingDirectories) ? 1 : 0);
            int rebalanceToStripe = maxFreeIndex + (numberOfWorkingDirectories * rand.nextInt(maxStripeCount));

            if (stripe % numberOfWorkingDirectories == minFreeIndex && rebalanceToStripe != stripe) {
                long sizeOfDirectory = FileUtils.sizeOfDirectory(baseKey(versionedPartitionName, stripe));
                if (sizeOfDirectory * 2 < rebalanceIfImbalanceGreaterThanInBytes) { // the times 2 says our index shouldn't be any bigger than our wal ;)
                    return rebalanceToStripe;
                }
            }
        }
        return -1;
    }

    public File baseKey(VersionedPartitionName versionedPartitionName, int stripe) {
        return new File(workingDirectories[stripe % workingDirectories.length], String.valueOf(versionedPartitionName.getPartitionVersion() % 1024));
    }

    public WALStorage<?> create(VersionedPartitionName versionedPartitionName, int stripe, PartitionProperties partitionProperties) throws Exception {
        return create(versionedPartitionName, partitionProperties);
    }

    public <I extends WALIndex> WALStorage<I> create(
        VersionedPartitionName versionedPartitionName,
        PartitionProperties partitionProperties) throws Exception {

        String providerName = partitionProperties.indexClassName;
        @SuppressWarnings("unchecked")
        WALIndexProvider<I> walIndexProvider = (WALIndexProvider<I>) indexProviderRegistry.getWALIndexProvider(providerName);
        @SuppressWarnings("unchecked")
        RowIOProvider rowIOProvider = indexProviderRegistry.getRowIOProvider(providerName);

        String name = (versionedPartitionName.getPartitionVersion() == VersionedPartitionName.STATIC_VERSION)
            ? versionedPartitionName.toBase64()
            : String.valueOf(versionedPartitionName.getPartitionVersion());

        BinaryWALTx binaryWALTx = new BinaryWALTx(
            name,
            rowIOProvider,
            primaryRowMarshaller,
            partitionProperties.updatesBetweenLeaps,
            partitionProperties.maxLeaps);
        boolean hardFsyncBeforeLeapBoundary = versionedPartitionName.getPartitionName().isSystemPartition();
        return new WALStorage<>(amzaStats,
            versionedPartitionName,
            orderIdProvider,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            binaryWALTx,
            walIndexProvider,
            sickPartitions,
            hardFsyncBeforeLeapBoundary,
            tombstoneCompactionFactor);
    }
}
