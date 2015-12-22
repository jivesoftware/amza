package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 *
 * @author jonathan.colt
 */
public class AmzaClientProvider<C, E extends Throwable> implements PartitionClientProvider {

    private final PartitionClientFactory<C, E> partitionClientFactory;
    private final PartitionHostsProvider partitionHostsProvider;
    private final RingHostClientProvider<C, E> clientProvider;
    private final ExecutorService callerThreads;
    private final long awaitLeaderElectionForNMillis;
    private final Map<PartitionName, PartitionClient> cache = new ConcurrentHashMap<>();

    public AmzaClientProvider(PartitionClientFactory<C, E> partitionClientFactory,
        PartitionHostsProvider partitionHostsProvider,
        RingHostClientProvider<C, E> clientProvider,
        ExecutorService callerThreads,
        long awaitLeaderElectionForNMillis) {
        this.partitionClientFactory = partitionClientFactory;
        this.partitionHostsProvider = partitionHostsProvider;
        this.clientProvider = clientProvider;
        this.callerThreads = callerThreads;
        this.awaitLeaderElectionForNMillis = awaitLeaderElectionForNMillis;
    }

    @Override
    public PartitionClient getPartition(PartitionName partitionName) throws Exception {
        PartitionClient got = cache.get(partitionName);
        if (got != null) {
            return got;
        }
        AmzaClientCallRouter<C, E> partitionCallRouter = new AmzaClientCallRouter<>(callerThreads, partitionHostsProvider, clientProvider);

        return partitionClientFactory.create(partitionName, partitionCallRouter, awaitLeaderElectionForNMillis);
    }

    @Override
    public PartitionClient getPartition(PartitionName partitionName,
        int ringSize,
        PartitionProperties partitionProperties) throws Exception {

        return cache.computeIfAbsent(partitionName, (key) -> {
            try {
                partitionHostsProvider.ensurePartition(partitionName, ringSize, partitionProperties);
                AmzaClientCallRouter<C, E> partitionCallRouter = new AmzaClientCallRouter<>(callerThreads, partitionHostsProvider, clientProvider);
                return partitionClientFactory.create(key, partitionCallRouter, awaitLeaderElectionForNMillis);
            } catch (Exception x) {
                throw new RuntimeException(x);
            }
        });
    }
}
