package com.jivesoftware.os.amza.service;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.RegionPropertyMarshaller;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALStorageProvider;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.RowIOProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;

/**
 *
 */
public class EmbeddedAmzaServiceInitializer {

    public AmzaService initialize(final AmzaServiceConfig config,
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
        final RowIOProvider rowIOProvider = new BinaryRowIOProvider(amzaStats.ioStats, config.corruptionParanoiaFactor, config.useMemMap);

        int tombstoneCompactionFactor = 2; // TODO expose to config;
        int compactAfterGrowthFactor = 2; // TODO expose to config;
        WALStorageProvider walStorageProvider = new IndexedWALStorageProvider(indexProviderRegistry,
            rowIOProvider, rowMarshaller, orderIdProvider, tombstoneCompactionFactor, compactAfterGrowthFactor);
        WALStorageProvider tmpWALStorageProvider = new TemporaryWALStorageProvider(rowIOProvider, rowMarshaller, orderIdProvider);

        return new AmzaServiceInitializer().initialize(config,
            amzaStats,
            rowMarshaller,
            ringHost,
            orderIdProvider,
            regionPropertyMarshaller,
            walStorageProvider,
            tmpWALStorageProvider,
            tmpWALStorageProvider,
            updatesSender,
            updatesTaker,
            sendFailureListener,
            takeFailureListener,
            allRowChanges);

    }

}
