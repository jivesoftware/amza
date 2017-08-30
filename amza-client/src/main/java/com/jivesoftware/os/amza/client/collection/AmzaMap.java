package com.jivesoftware.os.amza.client.collection;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
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
public class AmzaMap<K, V> {

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

    public AmzaMap(AmzaClientProvider clientProvider,
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

    private PartitionName partitionName() {
        byte[] nameAsBytes = partitionName.getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, nameAsBytes, nameAsBytes);
    }

    public void multiPut(Map<K, V> puts) throws Exception {
        PartitionClient partition = clientProvider.getPartition(partitionName(), ringSize, partitionProperties);
        long now = System.currentTimeMillis();
        partition.commit(Consistency.leader_quorum,
            null,
            (stream) -> {
                for (Entry<K, V> e : puts.entrySet()) {
                    stream.commit(keyMarshaller.toBytes(e.getKey()), valueMarshaller.toBytes(e.getValue()), now, false);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());

        LOG.info("Put ({}) {} configs.", puts.size(), partitionName);

    }

    public void multiRemove(List<K> keys) throws Exception {
        PartitionClient partition = clientProvider.getPartition(partitionName(), ringSize, partitionProperties);
        long now = System.currentTimeMillis();
        partition.commit(Consistency.leader_quorum,
            null,
            (stream) -> {
                for (K key : keys) {
                    stream.commit(keyMarshaller.toBytes(key), null, now, true);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());

        LOG.info("Removed ({}) {} configs.", keys.size(), partitionName);

    }

    public V get(K key) throws Exception {
        Object[] got = new Object[1];
        PartitionClient partition = clientProvider.getPartition(partitionName(), ringSize, partitionProperties);
        partition.get(Consistency.leader_quorum,
            null,
            (keyStream) -> keyStream.stream(keyMarshaller.toBytes(key)),
            (prefix, key1, value, timestamp, version) -> {
                if (value != null) {
                    got[0] = valueMarshaller.fromBytes(value);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty()
        );

        LOG.info("Got {} config.",partitionName);

        return (V) got[0];
    }

    public Map<K, V> multiGet(Collection<K> keys) throws Exception {
        if (keys.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<K, V> got = Maps.newConcurrentMap();
        PartitionClient partition = clientProvider.getPartition(partitionName(), ringSize, partitionProperties);
        partition.get(Consistency.leader_quorum,
            null,
            (keyStream) -> {
                for (K key : keys) {
                    if (!keyStream.stream(keyMarshaller.toBytes(key))) {
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

        LOG.info("Got ({}) {} configs.", got.size(), partitionName);

        return got;
    }

    public Map<K, V> getAll() throws Exception {
        Map<K, V> got = Maps.newConcurrentMap();
        PartitionClient partition = clientProvider.getPartition(partitionName(), ringSize, partitionProperties);
        partition.scan(Consistency.leader_quorum,
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

        LOG.info("Got All ({}) {} configs.", got.size(), partitionName);
        return got;
    }
}
