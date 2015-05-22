package com.jivesoftware.os.amza.service;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.shared.NoOpWALIndexProvider;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALStorageProvider;
import com.jivesoftware.os.amza.storage.NonIndexWAL;
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
class TemporaryWALStorageProvider implements WALStorageProvider {

    private final RowIOProvider rowIOProvider;
    private final BinaryPrimaryRowMarshaller primaryRowMarshaller;
    private final BinaryHighwaterRowMarshaller highwaterRowMarshaller;
    private final TimestampedOrderIdProvider orderIdProvider;

    public TemporaryWALStorageProvider(RowIOProvider rowIOProvider,
        BinaryPrimaryRowMarshaller rowMarshaller,
        BinaryHighwaterRowMarshaller highwaterRowMarshaller,
        TimestampedOrderIdProvider orderIdProvider) {
        this.rowIOProvider = rowIOProvider;
        this.primaryRowMarshaller = rowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.orderIdProvider = orderIdProvider;
    }

    @Override
    public WALStorage create(File workingDirectory, String domain, RegionName regionName, WALStorageDescriptor storageDescriptor) throws Exception {
        final File directory = new File(workingDirectory, domain);
        directory.mkdirs();
        BinaryWALTx rowsTx = new BinaryWALTx(directory, regionName.toBase64(), rowIOProvider, primaryRowMarshaller, new NoOpWALIndexProvider(), -1);
        rowsTx.validateAndRepair();
        return new NonIndexWAL(regionName, orderIdProvider, primaryRowMarshaller, highwaterRowMarshaller, rowsTx);
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
