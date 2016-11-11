package com.jivesoftware.os.amza.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 *
 */
public class AmzaSyncSenderInitializer {

    AmzaSyncSender initialize(AmzaSyncConfig syncConfig,
        AmzaClientAquariumProvider amzaClientAquariumProvider,
        PartitionClientProvider amzaClientProvider,
        ObjectMapper mapper,
        Map<PartitionName, PartitionName> whitelistPartitionNames,
        BAInterner interner) throws Exception {

        AmzaSyncClient syncClient = new AmzaSyncClientInitializer().initialize(syncConfig);

        return new AmzaSyncSender(amzaClientAquariumProvider,
            syncConfig.getSyncRingStripes(),
            Executors.newCachedThreadPool(),
            syncConfig.getSyncThreadCount(),
            syncConfig.getSyncIntervalMillis(),
            amzaClientProvider,
            syncClient,
            mapper,
            whitelistPartitionNames,
            syncConfig.getSyncBatchSize(),
            interner);
    }
}
