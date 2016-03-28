package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.PartitionStripeFunction;
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
import java.nio.charset.StandardCharsets;

/**
 * @author jonathan.colt
 */
public class IndexedWALStorageProvider {

    public static void main(String[] args) {
        PartitionStripeFunction f = new PartitionStripeFunction(3);
        int[] histo = new int[f.getNumberOfStripes()];
        for (int i = 0; i <= 160; i++) {
            byte[] ringName = ("activityWAL-global").getBytes(StandardCharsets.UTF_8);
            byte[] partitionName = ("activityWAL-global-" + i).getBytes(StandardCharsets.UTF_8);
            histo[f.stripe(new PartitionName(false, ringName, partitionName))]++;
        }
        for (int i = 0; i < f.getNumberOfStripes(); i++) {
            System.out.println("[" + (i + 1) + "] " + histo[i]);
        }
    }

    private final AmzaStats amzaStats;
    private final PartitionStripeFunction partitionStripeFunction;
    private final File[] workingDirectories;
    private final WALIndexProviderRegistry indexProviderRegistry;
    private final PrimaryRowMarshaller primaryRowMarshaller;
    private final BinaryHighwaterRowMarshaller highwaterRowMarshaller;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final SickPartitions sickPartitions;
    private final int tombstoneCompactionFactor;

    public IndexedWALStorageProvider(AmzaStats amzaStats,
        PartitionStripeFunction partitionStripeFunction,
        File[] workingDirectories,
        WALIndexProviderRegistry indexProviderRegistry,
        PrimaryRowMarshaller primaryRowMarshaller,
        BinaryHighwaterRowMarshaller highwaterRowMarshaller,
        TimestampedOrderIdProvider orderIdProvider,
        SickPartitions sickPartitions,
        int tombstoneCompactionFactor) {
        this.amzaStats = amzaStats;

        this.partitionStripeFunction = partitionStripeFunction;
        this.workingDirectories = workingDirectories;
        this.indexProviderRegistry = indexProviderRegistry;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.orderIdProvider = orderIdProvider;
        this.sickPartitions = sickPartitions;
        this.tombstoneCompactionFactor = tombstoneCompactionFactor;
    }

    public WALStorage<?> create(VersionedPartitionName versionedPartitionName, PartitionProperties partitionProperties) throws Exception {
        return create(baseKey(versionedPartitionName), versionedPartitionName, partitionProperties);
    }

    private File[] baseKey(VersionedPartitionName versionedPartitionName) {
        int keyStripe = partitionStripeFunction.stripe(versionedPartitionName.getPartitionName());
        int numberOfStripes = partitionStripeFunction.getNumberOfStripes();
        File[] striped = new File[numberOfStripes];
        String indexName = String.valueOf(versionedPartitionName.getPartitionVersion() % 1024);
        for (int i = 0; i < numberOfStripes; i++) {
            striped[i] = new File(workingDirectories[(keyStripe + i) % numberOfStripes], indexName);
        }
        return striped;
    }

    private <I extends WALIndex> WALStorage<I> create(File[] baseKey,
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

        // support stripe function changes by checking each stripe, favoring the current suggestion
        int index = 0;
        for (int i = 0; i < baseKey.length; i++) {
            if (baseKey[i].exists()) {
                index = i;
                break;
            }
        }

        BinaryWALTx binaryWALTx = new BinaryWALTx(baseKey[index],
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
