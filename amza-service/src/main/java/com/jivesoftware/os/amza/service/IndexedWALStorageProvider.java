package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.partition.WALStorageDescriptor;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALIndexProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;

/**
 * @author jonathan.colt
 */
public class IndexedWALStorageProvider {

    private final WALIndexProviderRegistry indexProviderRegistry;
    private final PrimaryRowMarshaller primaryRowMarshaller;
    private final BinaryHighwaterRowMarshaller highwaterRowMarshaller;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final int tombstoneCompactionFactor;

    public IndexedWALStorageProvider(WALIndexProviderRegistry indexProviderRegistry,
        PrimaryRowMarshaller primaryRowMarshaller,
        BinaryHighwaterRowMarshaller highwaterRowMarshaller,
        TimestampedOrderIdProvider orderIdProvider,
        int tombstoneCompactionFactor) {

        this.indexProviderRegistry = indexProviderRegistry;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.orderIdProvider = orderIdProvider;
        this.tombstoneCompactionFactor = tombstoneCompactionFactor;
    }

    public <I extends WALIndex, K> WALStorage<I> create(K baseKey,
        VersionedPartitionName versionedPartitionName,
        WALStorageDescriptor storageDescriptor) throws Exception {

        @SuppressWarnings("unchecked")
        WALIndexProvider<I> walIndexProvider = (WALIndexProvider<I>) indexProviderRegistry.getWALIndexProvider(storageDescriptor);
        @SuppressWarnings("unchecked")
        RowIOProvider<K> rowIOProvider = (RowIOProvider<K>) indexProviderRegistry.getRowIOProvider(storageDescriptor);
        BinaryWALTx<I, K> binaryWALTx = buildBinaryWALTx(baseKey, versionedPartitionName, rowIOProvider, walIndexProvider);
        return new WALStorage<>(versionedPartitionName,
            orderIdProvider,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            binaryWALTx,
            storageDescriptor.maxUpdatesBetweenCompactionHintMarker,
            storageDescriptor.maxUpdatesBetweenIndexCommitMarker,
            tombstoneCompactionFactor);
    }

    public WALStorage<?> create(VersionedPartitionName versionedPartitionName, WALStorageDescriptor walStorageDescriptor) throws Exception {
        return create(baseKey(versionedPartitionName, walStorageDescriptor), versionedPartitionName, walStorageDescriptor);
    }

    private <K> K baseKey(VersionedPartitionName versionedPartitionName, WALStorageDescriptor storageDescriptor) throws Exception {
        @SuppressWarnings("unchecked")
        RowIOProvider<K> rowIOProvider = (RowIOProvider<K>) indexProviderRegistry.getRowIOProvider(storageDescriptor);
        return rowIOProvider.baseKey(versionedPartitionName);
    }

    private <I extends WALIndex, K> BinaryWALTx<I, K> buildBinaryWALTx(K baseKey,
        VersionedPartitionName versionedPartitionName,
        RowIOProvider<K> rowIOProvider,
        WALIndexProvider<I> walIndexProvider) throws Exception {
        return new BinaryWALTx<>(baseKey,
            versionedPartitionName.toBase64(),
            rowIOProvider,
            primaryRowMarshaller,
            walIndexProvider);
    }
}
