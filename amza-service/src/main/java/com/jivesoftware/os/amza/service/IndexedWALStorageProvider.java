package com.jivesoftware.os.amza.service;

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
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;

/**
 * @author jonathan.colt
 */
public class IndexedWALStorageProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

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

        // support stripe function changes by moving misplaced files
        for (int i = 0; i < baseKey.length; i++) {
            if (baseKey[i].exists()) {
                if (i > 0) {

                }
                break;
            }
        }

        BinaryWALTx binaryWALTx = new BinaryWALTx(baseKey[0],
            name,
            rowIOProvider,
            primaryRowMarshaller,
            partitionProperties.updatesBetweenLeaps,
            partitionProperties.maxLeaps);
        if (!binaryWALTx.exists()) {
            for (int i = 1; i < baseKey.length; i++) {
                BinaryWALTx old = new BinaryWALTx(baseKey[i],
                    name,
                    rowIOProvider,
                    primaryRowMarshaller,
                    partitionProperties.updatesBetweenLeaps,
                    partitionProperties.maxLeaps);
                if (old.exists()) {
                    binaryWALTx.moveFrom(old);
                    break;
                }
            }
        }

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
