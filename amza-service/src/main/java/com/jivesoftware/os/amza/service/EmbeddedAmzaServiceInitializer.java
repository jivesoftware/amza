package com.jivesoftware.os.amza.service;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.RowsTaker;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;

/**
 *
 */
public class EmbeddedAmzaServiceInitializer {

    public AmzaService initialize(final AmzaServiceConfig config,
        AmzaStats amzaStats,
        RingMember ringMember,
        RingHost ringHost,
        TimestampedOrderIdProvider orderIdProvider,
        IdPacker idPacker,
        PartitionPropertyMarshaller partitionPropertyMarshaller,
        WALIndexProviderRegistry indexProviderRegistry,
        RowsTaker updatesTaker,
        Optional<TakeFailureListener> takeFailureListener,
        RowChanges allRowChanges) throws Exception {

        BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller(); // hehe you cant change this :)
        BinaryHighwaterRowMarshaller highwaterRowMarshaller = new BinaryHighwaterRowMarshaller();
        RowIOProvider rowIOProvider = new BinaryRowIOProvider(amzaStats.ioStats, config.corruptionParanoiaFactor, config.useMemMap);

        int tombstoneCompactionFactor = 2; // TODO expose to config;
        int compactAfterGrowthFactor = 2; // TODO expose to config;

        IndexedWALStorageProvider walStorageProvider = new IndexedWALStorageProvider(indexProviderRegistry,
            rowIOProvider, primaryRowMarshaller, highwaterRowMarshaller, orderIdProvider, tombstoneCompactionFactor, compactAfterGrowthFactor);


        AmzaService service = new AmzaServiceInitializer().initialize(config,
            amzaStats,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            ringMember,
            ringHost,
            orderIdProvider,
            idPacker,
            partitionPropertyMarshaller,
            walStorageProvider,
            updatesTaker,
            takeFailureListener,
            allRowChanges);

        return service;

    }

}
