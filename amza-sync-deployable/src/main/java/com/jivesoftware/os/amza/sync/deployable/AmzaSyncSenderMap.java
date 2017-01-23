package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.collection.AmzaMarshaller;
import com.jivesoftware.os.amza.client.collection.AmzaMap;
import com.jivesoftware.os.amza.sync.api.AmzaSyncSenderConfig;

/**
 *
 */
public class AmzaSyncSenderMap extends AmzaMap<String, AmzaSyncSenderConfig> implements AmzaSyncSenderConfigProvider {

    public AmzaSyncSenderMap(AmzaClientProvider clientProvider,
        String partitionName,
        PartitionProperties partitionProperties,
        AmzaMarshaller<String> keyMarshaller,
        AmzaMarshaller<AmzaSyncSenderConfig> valueMarshaller) {
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
