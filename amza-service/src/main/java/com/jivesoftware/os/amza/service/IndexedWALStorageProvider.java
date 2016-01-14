package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.api.partition.PartitionStripeFunction;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.partition.WALStorageDescriptor;
import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.WALIndex;
import com.jivesoftware.os.amza.api.wal.WALIndexProvider;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import java.io.File;

/**
 * @author jonathan.colt
 */
public class IndexedWALStorageProvider {

    private final PartitionStripeFunction partitionStripeFunction;
    private final File[] workingDirectories;
    private final WALIndexProviderRegistry indexProviderRegistry;
    private final PrimaryRowMarshaller primaryRowMarshaller;
    private final BinaryHighwaterRowMarshaller highwaterRowMarshaller;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final SickPartitions sickPartitions;
    private final int tombstoneCompactionFactor;

    public IndexedWALStorageProvider(PartitionStripeFunction partitionStripeFunction,
        File[] workingDirectories,
        WALIndexProviderRegistry indexProviderRegistry,
        PrimaryRowMarshaller primaryRowMarshaller,
        BinaryHighwaterRowMarshaller highwaterRowMarshaller,
        TimestampedOrderIdProvider orderIdProvider,
        SickPartitions sickPartitions,
        int tombstoneCompactionFactor) {

        this.partitionStripeFunction = partitionStripeFunction;
        this.workingDirectories = workingDirectories;
        this.indexProviderRegistry = indexProviderRegistry;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.orderIdProvider = orderIdProvider;
        this.sickPartitions = sickPartitions;
        this.tombstoneCompactionFactor = tombstoneCompactionFactor;
    }

    public File baseKey(VersionedPartitionName versionedPartitionName) {
        return workingDirectories[partitionStripeFunction.stripe(versionedPartitionName.getPartitionName())];
    }

    public WALStorage<?> create(VersionedPartitionName versionedPartitionName, WALStorageDescriptor walStorageDescriptor) throws Exception {
        return create(baseKey(versionedPartitionName), versionedPartitionName, walStorageDescriptor);
    }

    public <I extends WALIndex> WALStorage<I> create(File baseKey,
        VersionedPartitionName versionedPartitionName,
        WALStorageDescriptor storageDescriptor) throws Exception {

        String providerName = storageDescriptor.primaryIndexDescriptor.className;
        @SuppressWarnings("unchecked")
        WALIndexProvider<I> walIndexProvider = (WALIndexProvider<I>) indexProviderRegistry.getWALIndexProvider(providerName);
        @SuppressWarnings("unchecked")
        RowIOProvider rowIOProvider = indexProviderRegistry.getRowIOProvider(providerName);
        BinaryWALTx binaryWALTx = new BinaryWALTx(baseKey,
            versionedPartitionName.toBase64(),
            rowIOProvider,
            primaryRowMarshaller);
        return new WALStorage<>(versionedPartitionName,
            orderIdProvider,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            binaryWALTx,
            walIndexProvider,
            sickPartitions,
            storageDescriptor.maxUpdatesBetweenCompactionHintMarker,
            storageDescriptor.maxUpdatesBetweenIndexCommitMarker,
            tombstoneCompactionFactor);
    }
}
