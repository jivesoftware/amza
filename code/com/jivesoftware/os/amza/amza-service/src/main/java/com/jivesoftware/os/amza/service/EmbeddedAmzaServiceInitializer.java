package com.jivesoftware.os.amza.service;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.RegionPropertyMarshaller;
import com.jivesoftware.os.amza.shared.NoOpWALIndexProvider;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALIndexProvider;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALStorageProvider;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.storage.IndexedWAL;
import com.jivesoftware.os.amza.storage.NonIndexWAL;
import com.jivesoftware.os.amza.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.storage.binary.RowIOProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 *
 */
public class EmbeddedAmzaServiceInitializer {

    public AmzaService initialize(final AmzaServiceConfig amzaServiceConfig,
        final AmzaStats amzaStats,
        RingHost ringHost,
        final TimestampedOrderIdProvider orderIdProvider,
        RegionPropertyMarshaller regionPropertyMarshaller,
        final WALIndexProviderRegistry indexProviderRegistry,
        UpdatesSender updatesSender,
        UpdatesTaker updatesTaker,
        Optional<SendFailureListener> sendFailureListener,
        Optional<TakeFailureListener> takeFailureListener,
        final RowChanges allRowChanges) throws Exception {

        final BinaryRowMarshaller rowMarshaller = new BinaryRowMarshaller();
        final RowIOProvider rowIOProvider = new BinaryRowIOProvider(amzaStats.ioStats);

        WALStorageProvider regionStorageProvider = new WALStorageProvider() {
            @Override
            public WALStorage create(File workingDirectory,
                String domain,
                RegionName regionName,
                WALStorageDescriptor storageDescriptor,
                WALReplicator rowReplicator) throws Exception {

                WALIndexProvider walIndexProvider = indexProviderRegistry.getWALIndexProvider(storageDescriptor);

                final File directory = new File(workingDirectory, domain);
                directory.mkdirs();
                return new IndexedWAL(regionName,
                    orderIdProvider,
                    rowMarshaller,
                    new BinaryWALTx(directory,
                        regionName.toBase64(),
                        rowIOProvider,
                        rowMarshaller,
                        walIndexProvider),
                    rowReplicator,
                    storageDescriptor.maxUpdatesBetweenCompactionHintMarker,
                    storageDescriptor.maxUpdatesBetweenIndexCommitMarker);
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
        };

        WALStorageProvider tmpWALStorageProvider = new WALStorageProvider() {
            @Override
            public WALStorage create(File workingDirectory,
                String domain,
                RegionName regionName,
                WALStorageDescriptor storageDescriptor,
                WALReplicator rowReplicator) throws Exception {

                final File directory = new File(workingDirectory, domain);
                directory.mkdirs();
                return new NonIndexWAL(regionName,
                    orderIdProvider,
                    rowMarshaller,
                    new BinaryWALTx(directory,
                        regionName.toBase64(),
                        rowIOProvider,
                        rowMarshaller,
                        new NoOpWALIndexProvider()));
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
        };

        return new AmzaServiceInitializer().initialize(amzaServiceConfig,
            amzaStats,
            rowMarshaller,
            ringHost,
            orderIdProvider,
            regionPropertyMarshaller,
            regionStorageProvider,
            tmpWALStorageProvider,
            tmpWALStorageProvider,
            updatesSender,
            updatesTaker,
            sendFailureListener,
            takeFailureListener,
            allRowChanges);

    }
}
