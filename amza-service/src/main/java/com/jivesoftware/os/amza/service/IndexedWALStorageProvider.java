package com.jivesoftware.os.amza.service;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALIndexProvider;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * @author jonathan.colt
 */
public class IndexedWALStorageProvider {

    private final WALIndexProviderRegistry indexProviderRegistry;
    private final RowIOProvider rowIOProvider;
    private final BinaryPrimaryRowMarshaller primaryRowMarshaller;
    private final BinaryHighwaterRowMarshaller highwaterRowMarshaller;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final int tombstoneCompactionFactor;

    public IndexedWALStorageProvider(WALIndexProviderRegistry indexProviderRegistry,
        RowIOProvider rowIOProvider,
        BinaryPrimaryRowMarshaller primaryRowMarshaller,
        BinaryHighwaterRowMarshaller highwaterRowMarshaller,
        TimestampedOrderIdProvider orderIdProvider,
        int tombstoneCompactionFactor) {

        this.indexProviderRegistry = indexProviderRegistry;
        this.rowIOProvider = rowIOProvider;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.orderIdProvider = orderIdProvider;
        this.tombstoneCompactionFactor = tombstoneCompactionFactor;
    }

    public <I extends WALIndex> WALStorage<I> create(File workingDirectory,
        String domain,
        VersionedPartitionName versionedPartitionName,
        WALStorageDescriptor storageDescriptor) throws Exception {
        @SuppressWarnings("unchecked")
        WALIndexProvider<I> walIndexProvider = (WALIndexProvider<I>) indexProviderRegistry.getWALIndexProvider(storageDescriptor);
        File directory = new File(workingDirectory, domain);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new IOException("Failed trying to mkdirs for " + directory);
        }
        BinaryWALTx<I> binaryWALTx = new BinaryWALTx<>(directory, versionedPartitionName.toBase64(), rowIOProvider, primaryRowMarshaller, walIndexProvider);
        return new WALStorage<>(versionedPartitionName,
            orderIdProvider,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            binaryWALTx,
            storageDescriptor.maxUpdatesBetweenCompactionHintMarker,
            storageDescriptor.maxUpdatesBetweenIndexCommitMarker,
            tombstoneCompactionFactor);
    }

    public Set<VersionedPartitionName> listExisting(String[] workingDirectories, String domain) throws IOException {
        Set<VersionedPartitionName> versionedPartitionNames = Sets.newHashSet();
        for (String workingDirectory : workingDirectories) {
            File directory = new File(workingDirectory, domain);
            if (directory.exists() && directory.isDirectory()) {
                Set<String> partitions = BinaryWALTx.listExisting(directory, rowIOProvider);
                for (String partition : partitions) {
                    versionedPartitionNames.add(VersionedPartitionName.fromBase64(partition));
                }
            }
        }
        return versionedPartitionNames;
    }

}
