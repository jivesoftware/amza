package com.jivesoftware.os.amza.service;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.WALIndexProvider;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALStorageProvider;
import com.jivesoftware.os.amza.storage.IndexedWAL;
import com.jivesoftware.os.amza.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.storage.binary.RowIOProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class IndexedWALStorageProvider implements WALStorageProvider {

    private final WALIndexProviderRegistry indexProviderRegistry;
    private final RowIOProvider rowIOProvider;
    private final BinaryPrimaryRowMarshaller primaryRowMarshaller;
    private final BinaryHighwaterRowMarshaller highwaterRowMarshaller;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final int tombstoneCompactionFactor;
    private final int compactAfterGrowthFactor;

    public IndexedWALStorageProvider(WALIndexProviderRegistry indexProviderRegistry,
        RowIOProvider rowIOProvider,
        BinaryPrimaryRowMarshaller primaryRowMarshaller,
        BinaryHighwaterRowMarshaller highwaterRowMarshaller,
        TimestampedOrderIdProvider orderIdProvider,
        int tombstoneCompactionFactor,
        int compactAfterGrowthFactor) {
        this.indexProviderRegistry = indexProviderRegistry;
        this.rowIOProvider = rowIOProvider;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.orderIdProvider = orderIdProvider;
        this.tombstoneCompactionFactor = tombstoneCompactionFactor;
        this.compactAfterGrowthFactor = compactAfterGrowthFactor;
    }

    @Override
    public WALStorage create(File workingDirectory,
        String domain,
        RegionName regionName,
        WALStorageDescriptor storageDescriptor) throws Exception {
        WALIndexProvider walIndexProvider = indexProviderRegistry.getWALIndexProvider(storageDescriptor);
        final File directory = new File(workingDirectory, domain);
        directory.mkdirs();
        BinaryWALTx binaryWALTx = new BinaryWALTx(directory, regionName.toBase64(), rowIOProvider, primaryRowMarshaller, walIndexProvider,
            compactAfterGrowthFactor);
        return new IndexedWAL(regionName,
            orderIdProvider,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            binaryWALTx,
            storageDescriptor.maxUpdatesBetweenCompactionHintMarker,
            storageDescriptor.maxUpdatesBetweenIndexCommitMarker,
            tombstoneCompactionFactor);
    }

    @Override
    public Set<RegionName> listExisting(String[] workingDirectories, String domain) throws IOException {
        Set<RegionName> regionNames = Sets.newHashSet();
        for (String workingDirectory : workingDirectories) {
            File directory = new File(workingDirectory, domain);
            if (directory.exists() && directory.isDirectory()) {
                Set<String> regions = BinaryWALTx.listExisting(directory, rowIOProvider);
                for (String region : regions) {
                    regionNames.add(RegionName.fromBase64(region));
                }
            }
        }
        return regionNames;
    }

}
