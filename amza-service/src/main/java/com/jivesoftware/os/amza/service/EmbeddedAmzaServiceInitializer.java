package com.jivesoftware.os.amza.service;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.RegionPropertyMarshaller;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker;
import com.jivesoftware.os.amza.shared.wal.WALStorageProvider;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.storage.binary.RowIOProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;

/**
 *
 */
public class EmbeddedAmzaServiceInitializer {

    public AmzaService initialize(final AmzaServiceConfig config,
        final AmzaStats amzaStats,
        RingMember ringMember,
        RingHost ringHost,
        final TimestampedOrderIdProvider orderIdProvider,
        RegionPropertyMarshaller regionPropertyMarshaller,
        final WALIndexProviderRegistry indexProviderRegistry,
        UpdatesTaker updatesTaker,
        Optional<SendFailureListener> sendFailureListener,
        Optional<TakeFailureListener> takeFailureListener,
        final RowChanges allRowChanges) throws Exception {

        final BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller(); // hehe you cant change this :)
        final BinaryHighwaterRowMarshaller highwaterRowMarshaller = new BinaryHighwaterRowMarshaller();
        final RowIOProvider rowIOProvider = new BinaryRowIOProvider(amzaStats.ioStats, config.corruptionParanoiaFactor, config.useMemMap);

        int tombstoneCompactionFactor = 2; // TODO expose to config;
        int compactAfterGrowthFactor = 2; // TODO expose to config;
        WALStorageProvider walStorageProvider = new IndexedWALStorageProvider(indexProviderRegistry,
            rowIOProvider, primaryRowMarshaller, highwaterRowMarshaller, orderIdProvider, tombstoneCompactionFactor, compactAfterGrowthFactor);

        return new AmzaServiceInitializer().initialize(config,
            amzaStats,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            ringMember,
            ringHost,
            orderIdProvider,
            regionPropertyMarshaller,
            walStorageProvider,
            updatesTaker,
            sendFailureListener,
            takeFailureListener,
            allRowChanges);

    }

}
