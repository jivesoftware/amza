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
public class AmzaSyncSenderConfigStorage implements AmzaSyncSenderConfigProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ObjectMapper mapper;
    private final BAInterner interner;
    private final AmzaClientProvider<HttpClient, HttpClientException> clientProvider;

    private final PartitionProperties partitionProperties;
    private final long additionalSolverAfterNMillis = 1_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 5_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 30_000; //TODO expose to conf?

    public AmzaSyncSenderConfigStorage(BAInterner interner,
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

    public void multiPutIfAbsent(Map<String, AmzaSyncSenderConfig> senders) throws Exception {
        Map<String, AmzaSyncSenderConfig> got = multiGet(senders.keySet());
        Map<String, AmzaSyncSenderConfig> put = Maps.newHashMap();
        for (Entry<String, AmzaSyncSenderConfig> entry : senders.entrySet()) {
            if (!got.containsKey(entry.getKey())) {
                put.put(entry.getKey(), entry.getValue());
            }
        }
        if (!put.isEmpty()) {
            multiPut(put);
        }
    }

    public void multiPut(Map<String, AmzaSyncSenderConfig> configs) throws Exception {

        PartitionClient partition = clientProvider.getPartition(partitionName(), 3, partitionProperties);
        long now = System.currentTimeMillis();
        partition.commit(Consistency.leader_quorum,
            null,
            (stream) -> {
                for (Entry<String, AmzaSyncSenderConfig> configEntry : configs.entrySet()) {
                    stream.commit(configEntry.getKey().getBytes(StandardCharsets.UTF_8), mapper.writeValueAsBytes(configEntry.getValue()), now, false);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());

        LOG.info("Put {} sender configs.", configs.size());

    }

    public void multiRemove(List<String> senderNames) throws Exception {

        PartitionClient partition = clientProvider.getPartition(partitionName(), 3, partitionProperties);
        long now = System.currentTimeMillis();
        partition.commit(Consistency.leader_quorum,
            null,
            (stream) -> {
                for (String senderName : senderNames) {
                    stream.commit(senderName.getBytes(StandardCharsets.UTF_8), null, now, true);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());

        LOG.info("Removed {} sender configs.", senderNames.size());

    }

    public Map<String, AmzaSyncSenderConfig> multiGet(Collection<String> senderNames) throws Exception {
        if (senderNames.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, AmzaSyncSenderConfig> got = Maps.newConcurrentMap();
        PartitionClient partition = clientProvider.getPartition(partitionName(), 3, partitionProperties);
        partition.get(Consistency.leader_quorum,
            null,
            (keyStream) -> {
                for (String senderName : senderNames) {
                    if (!keyStream.stream(senderName.getBytes(StandardCharsets.UTF_8))) {
                        return false;
                    }
                }
                return true;
            },
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    got.put(new String(key, StandardCharsets.UTF_8), mapper.readValue(value, AmzaSyncSenderConfig.class));
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty()
        );

        LOG.info("Got {} sender configs.", got.size());

        return got;
    }

    public Map<String, AmzaSyncSenderConfig> getAll() throws Exception {
        Map<String, AmzaSyncSenderConfig> got = Maps.newConcurrentMap();
        PartitionClient partition = clientProvider.getPartition(partitionName(), 3, partitionProperties);
        partition.scan(Consistency.leader_quorum,
            false,
            null,
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    got.put(new String(key, StandardCharsets.UTF_8), mapper.readValue(value, AmzaSyncSenderConfig.class));
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
