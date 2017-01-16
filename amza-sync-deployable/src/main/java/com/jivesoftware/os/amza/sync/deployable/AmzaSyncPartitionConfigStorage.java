package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.sync.api.AmzaConfigMarshaller;
import com.jivesoftware.os.amza.sync.api.AmzaPartitionedConfigStorage;
import com.jivesoftware.os.amza.sync.api.AmzaSyncPartitionConfig;
import com.jivesoftware.os.amza.sync.api.AmzaSyncPartitionTuple;

/**
 *
 */
public class AmzaSyncPartitionConfigStorage extends AmzaPartitionedConfigStorage<AmzaSyncPartitionTuple, AmzaSyncPartitionConfig> implements
    AmzaSyncPartitionConfigProvider {


    public AmzaSyncPartitionConfigStorage(AmzaClientProvider clientProvider,
        String partitionName,
        PartitionProperties partitionProperties,
        AmzaConfigMarshaller<AmzaSyncPartitionTuple> keyMarshaller,
        AmzaConfigMarshaller<AmzaSyncPartitionConfig> valueMarshaller) {
        super(clientProvider, partitionName, partitionProperties, keyMarshaller, valueMarshaller);
    }

    /*
    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ObjectMapper mapper;
    private final BAInterner interner;
    private final AmzaClientProvider<HttpClient, HttpClientException> clientProvider;

    private final PartitionProperties partitionProperties;
    private final long additionalSolverAfterNMillis = 1_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 5_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 30_000; //TODO expose to conf?

    public AmzaSyncPartitionConfigStorage(BAInterner interner,
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

    private PartitionName partitionName(String senderName) {
        byte[] nameAsBytes = ("amza-sync-partitions-config-" + senderName).getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, nameAsBytes, nameAsBytes);
    }*/


}
