package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.sync.api.AmzaConfigMarshaller;
import com.jivesoftware.os.amza.sync.api.AmzaConfigStorage;
import com.jivesoftware.os.amza.sync.api.AmzaSyncSenderConfig;

/**
 *
 */
public class AmzaSyncSenderConfigStorage extends AmzaConfigStorage<String, AmzaSyncSenderConfig> implements AmzaSyncSenderConfigProvider {

    public AmzaSyncSenderConfigStorage(AmzaClientProvider clientProvider,
        String partitionName,
        PartitionProperties partitionProperties,
        AmzaConfigMarshaller<String> keyMarshaller,
        AmzaConfigMarshaller<AmzaSyncSenderConfig> valueMarshaller) {
        super(clientProvider,partitionName,partitionProperties,keyMarshaller,valueMarshaller);
    }

   /* public AmzaSyncSenderConfigStorage(BAInterner interner,
        ObjectMapper mapper,
        TenantAwareHttpClient<String> httpClient,
        long awaitLeaderElectionForNMillis) {

        this.interner = interner;
        this.mapper = mapper;

        this.clientProvider = new AmzaClientProvider<>(
            new HttpPartitionClientFactory(this.interner),
            new HttpPartitionHostsProvider(this.interner, httpClient, mapper),
            new RingHostHttpClientProvider(httpClient),
            Executors.newCachedThreadPool(), //TODO expose to conf?
            awaitLeaderElectionForNMillis,
            -1,
            -1);

        partitionProperties = new PartitionProperties(Durability.fsync_async,
            0, 0, 0, 0, 0, 0, 0, 0,
            false,
            Consistency.leader_quorum,
            true,
            true,
            false,
            RowType.snappy_primary,
            "lab",
            -1,
            null,
            -1,
            -1);
    }

    private PartitionName partitionName() {
        byte[] nameAsBytes = ("amza-sync-sender-config").getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, nameAsBytes, nameAsBytes);
    }
*/

}
