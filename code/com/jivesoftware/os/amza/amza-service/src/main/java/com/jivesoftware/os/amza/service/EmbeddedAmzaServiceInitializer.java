package com.jivesoftware.os.amza.service;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.replication.MemoryBackedHighWaterMarks;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.NoOpWALIndex;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALIndex;
import com.jivesoftware.os.amza.shared.WALIndexProvider;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageProvider;
import com.jivesoftware.os.amza.storage.IndexedWAL;
import com.jivesoftware.os.amza.storage.NonIndexWAL;
import com.jivesoftware.os.amza.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.storage.binary.RowIOProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import java.io.File;

/**
 *
 */
public class EmbeddedAmzaServiceInitializer {

    public AmzaService initialize(AmzaServiceConfig amzaServiceConfig,
        AmzaStats amzaStats,
        RingHost ringHost,
        final TimestampedOrderIdProvider orderIdProvider,
        final WALIndexProvider walIndexProvider,
        UpdatesSender updatesSender,
        UpdatesTaker updatesTaker,
        Optional<SendFailureListener> sendFailureListener,
        Optional<TakeFailureListener> takeFailureListener,
        final RowChanges allRowChanges) throws Exception {

        final BinaryRowMarshaller rowMarshaller = new BinaryRowMarshaller();

        final int backRepairIndexForNDistinctTxIds = 1000;
        WALStorageProvider regionStorageProvider = new WALStorageProvider() {
            @Override
            public WALStorage create(File workingDirectory,
                String domain,
                RegionName regionName,
                WALReplicator rowReplicator) throws Exception {

                final File directory = new File(workingDirectory, domain);
                directory.mkdirs();
                RowIOProvider rowIOProvider = new BinaryRowIOProvider();
                return new IndexedWAL(regionName,
                    orderIdProvider,
                    rowMarshaller,
                    new BinaryWALTx(directory,
                        regionName.getRegionName() + ".kvt",
                        rowIOProvider,
                        rowMarshaller,
                        walIndexProvider,
                        backRepairIndexForNDistinctTxIds),
                    rowReplicator,
                    1000);
            }
        };

        final WALIndexProvider tmpWALIndexProvider = new WALIndexProvider() {

            @Override
            public WALIndex createIndex(RegionName regionName) throws Exception {
                return new NoOpWALIndex();
            }
        };

        WALStorageProvider tmpWALStorageProvider = new WALStorageProvider() {
            @Override
            public WALStorage create(File workingDirectory,
                String domain,
                RegionName regionName,
                WALReplicator rowReplicator) throws Exception {

                final File directory = new File(workingDirectory, domain);
                directory.mkdirs();
                RowIOProvider rowIOProvider = new BinaryRowIOProvider();
                return new NonIndexWAL(regionName,
                    orderIdProvider,
                    rowMarshaller,
                    new BinaryWALTx(directory,
                        regionName.getRegionName() + ".kvt",
                        rowIOProvider,
                        rowMarshaller,
                        tmpWALIndexProvider,
                        backRepairIndexForNDistinctTxIds));
            }
        };

        MemoryBackedHighWaterMarks highWaterMarks = new MemoryBackedHighWaterMarks();
        return new AmzaServiceInitializer().initialize(amzaServiceConfig,
            amzaStats,
            ringHost,
            orderIdProvider,
            regionStorageProvider,
            tmpWALStorageProvider,
            tmpWALStorageProvider,
            updatesSender,
            updatesTaker,
            highWaterMarks,
            sendFailureListener,
            takeFailureListener,
            allRowChanges);

    }
}
