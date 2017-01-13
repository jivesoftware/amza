package com.jivesoftware.os.amza.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.amza.sync.api.AmzaSyncPartitionConfig;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.Executors;

/**
 *
 */
public class AmzaSyncPartitionConfigStorage implements AmzaSyncPartitionConfigProvider {

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
        byte[] nameAsBytes = ("amza-sync-partition-config-" + senderName).getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, nameAsBytes, nameAsBytes);
    }

    public void multiPutIfAbsent(String senderName, Map<PartitionName, AmzaSyncPartitionConfig> whitelistTenantIds) throws Exception {
        Map<PartitionName, AmzaSyncPartitionConfig> got = multiGet(senderName, whitelistTenantIds.keySet());
        Map<PartitionName, AmzaSyncPartitionConfig> put = Maps.newHashMap();
        for (Entry<PartitionName, AmzaSyncPartitionConfig> entry : whitelistTenantIds.entrySet()) {
            if (!got.containsKey(entry.getKey())) {
                put.put(entry.getKey(), entry.getValue());
            }
        }
        if (!put.isEmpty()) {
            multiPut(senderName, put);
        }
    }

    public void multiPut(String senderName, Map<PartitionName, AmzaSyncPartitionConfig> configs) throws Exception {

        PartitionClient partition = clientProvider.getPartition(partitionName(senderName), 3, partitionProperties);
        long now = System.currentTimeMillis();
        partition.commit(Consistency.leader_quorum,
            null,
            (stream) -> {
                for (Entry<PartitionName, AmzaSyncPartitionConfig> configEntry : configs.entrySet()) {
                    stream.commit(configEntry.getKey().toBytes(), mapper.writeValueAsBytes(configEntry.getValue()), now, false);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());

        LOG.info("Put {} configs.", configs.size());

    }

    public void multiRemove(String senderName, List<PartitionName> partitionNames) throws Exception {

        PartitionClient partition = clientProvider.getPartition(partitionName(senderName), 3, partitionProperties);
        long now = System.currentTimeMillis();
        partition.commit(Consistency.leader_quorum,
            null,
            (stream) -> {
                for (PartitionName partitionName : partitionNames) {
                    stream.commit(partitionName.toBytes(), null, now, true);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());

        LOG.info("Removed {} configs.", partitionNames.size());

    }

    public Map<PartitionName, AmzaSyncPartitionConfig> multiGet(String senderName, Collection<PartitionName> partitionNames) throws Exception {
        if (partitionNames.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<PartitionName, AmzaSyncPartitionConfig> got = Maps.newConcurrentMap();
        PartitionClient partition = clientProvider.getPartition(partitionName(senderName), 3, partitionProperties);
        partition.get(Consistency.leader_quorum,
            null,
            (keyStream) -> {
                for (PartitionName partitionName : partitionNames) {
                    if (!keyStream.stream(partitionName.toBytes())) {
                        return false;
                    }
                }
                return true;
            },
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    got.put(PartitionName.fromBytes(key, 0, interner), mapper.readValue(value, AmzaSyncPartitionConfig.class));
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty()
        );

        LOG.info("Got {} configs.", got.size());

        return got;
    }

    public Map<PartitionName, AmzaSyncPartitionConfig> getAll(String senderName) throws Exception {
        Map<PartitionName, AmzaSyncPartitionConfig> got = Maps.newConcurrentMap();
        PartitionClient partition = clientProvider.getPartition(partitionName(senderName), 3, partitionProperties);
        partition.scan(Consistency.leader_quorum,
            false,
            null,
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    got.put(PartitionName.fromBytes(key, 0, interner), mapper.readValue(value, AmzaSyncPartitionConfig.class));
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty()
        );

        LOG.info("Got All {} configs.", got.size());
        return got;
    }


}
