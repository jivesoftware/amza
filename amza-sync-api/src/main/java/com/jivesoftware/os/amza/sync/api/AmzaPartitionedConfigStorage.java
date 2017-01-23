package com.jivesoftware.os.amza.sync.api;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.client.collection.AmzaMarshaller;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Created by jonathan.colt on 1/16/17.
 */
public class AmzaPartitionedConfigStorage<K, V> {
    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaClientProvider<HttpClient, HttpClientException> clientProvider;

    private final PartitionProperties partitionProperties;
    private final int ringSize = 3; //TODO expose to conf?
    private final String partitionName;
    private final AmzaMarshaller<K> keyMarshaller;
    private final AmzaMarshaller<V> valueMarshaller;

    private final long additionalSolverAfterNMillis = 1_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 5_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 30_000; //TODO expose to conf?

    public AmzaPartitionedConfigStorage(AmzaClientProvider clientProvider,
        String partitionName,
        PartitionProperties partitionProperties,
        AmzaMarshaller<K> keyMarshaller,
        AmzaMarshaller<V> valueMarshaller) {

        this.clientProvider = clientProvider;
        this.partitionProperties = partitionProperties;
        this.partitionName = partitionName;
        this.keyMarshaller = keyMarshaller;
        this.valueMarshaller = valueMarshaller;

    }

    private PartitionName partitionName(String senderName) {
        byte[] nameAsBytes = (partitionName + senderName).getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, nameAsBytes, nameAsBytes);
    }

    public void multiPutIfAbsent(String senderName, Map<K, V> puts) throws Exception {
        Map<K, V> got = multiGet(senderName, puts.keySet());
        Map<K, V> put = Maps.newHashMap();
        for (Entry<K, V> entry : puts.entrySet()) {
            if (!got.containsKey(entry.getKey())) {
                put.put(entry.getKey(), entry.getValue());
            }
        }
        if (!put.isEmpty()) {
            multiPut(senderName, put);
        }
    }

    public void multiPut(String senderName, Map<K, V> configs) throws Exception {

        PartitionClient client = clientProvider.getPartition(partitionName(senderName), ringSize, partitionProperties);
        long now = System.currentTimeMillis();
        client.commit(Consistency.leader_quorum,
            null,
            (stream) -> {
                for (Entry<K, V> e : configs.entrySet()) {
                    stream.commit(keyMarshaller.toBytes(e.getKey()),valueMarshaller.toBytes(e.getValue()), now, false);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());

        LOG.info("Put {} configs.", configs.size());

    }

    public void multiRemove(String senderName, List<K> keys) throws Exception {

        PartitionClient client = clientProvider.getPartition(partitionName(senderName), ringSize, partitionProperties);
        long now = System.currentTimeMillis();
        client.commit(Consistency.leader_quorum,
            null,
            (stream) -> {
                for (K tupleName : keys) {
                    stream.commit(keyMarshaller.toBytes(tupleName), null, now, true);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());

        LOG.info("Removed {} configs.", keys.size());

    }

    public Map<K, V> multiGet(String senderName, Collection<K> tupleNames) throws Exception {
        if (tupleNames.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<K, V> got = Maps.newConcurrentMap();
        PartitionClient client = clientProvider.getPartition(partitionName(senderName), ringSize, partitionProperties);
        client.get(Consistency.leader_quorum,
            null,
            (keyStream) -> {
                for (K tupleName : tupleNames) {
                    if (!keyStream.stream(keyMarshaller.toBytes(tupleName))) {
                        return false;
                    }
                }
                return true;
            },
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    got.put(keyMarshaller.fromBytes(key), valueMarshaller.fromBytes(value));
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

    public Map<K, V> getAll(String senderName) throws Exception {
        Map<K, V> got = Maps.newConcurrentMap();
        PartitionClient client = clientProvider.getPartition(partitionName(senderName), ringSize, partitionProperties);
        client.scan(Consistency.leader_quorum,
            false,
            stream -> stream.stream(null, null, null, null),
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    got.put(keyMarshaller.fromBytes(key), valueMarshaller.fromBytes(value));
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
