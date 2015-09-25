package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 *
 * @author jonathan.colt
 */
public class AmzaHttpClientProvider implements PartitionClientProvider {

    private final PartitionHostsProvider partitionHostsProvider;
    private final RingHostHttpClientProvider clientProvider;
    private final ExecutorService callerThreads;
    private final long awaitLeaderElectionForNMillis;
    private final Map<PartitionName, AmzaHttpPartitionClient> cache = new ConcurrentHashMap<>();

    public AmzaHttpClientProvider(PartitionHostsProvider partitionHostsProvider,
        RingHostHttpClientProvider clientProvider,
        ExecutorService callerThreads,
        long awaitLeaderElectionForNMillis) {
        this.partitionHostsProvider = partitionHostsProvider;
        this.clientProvider = clientProvider;
        this.callerThreads = callerThreads;
        this.awaitLeaderElectionForNMillis = awaitLeaderElectionForNMillis;
    }

    @Override
    public PartitionClient getPartition(PartitionName partitionName) throws Exception {

        return cache.computeIfAbsent(partitionName, (key) -> {
            try {
                AmzaHttpClientCallRouter partitionCallRouter = new AmzaHttpClientCallRouter(callerThreads, partitionHostsProvider, clientProvider);
                return new AmzaHttpPartitionClient(key, partitionCallRouter, awaitLeaderElectionForNMillis);
            } catch (Exception x) {
                throw new RuntimeException(x);
            }
        });
    }

}
