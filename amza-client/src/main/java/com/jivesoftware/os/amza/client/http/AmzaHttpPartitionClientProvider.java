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
public class AmzaHttpPartitionClientProvider implements PartitionClientProvider {

    private final PartitionHostsProvider partitionHostsProvider;
    private final ExecutorService callerThreads;
    private final Map<PartitionName, AmzaHttpPartitionClient> cache = new ConcurrentHashMap<>();

    public AmzaHttpPartitionClientProvider(PartitionHostsProvider partitionHostsProvider,
        ExecutorService callerThreads) {
        this.partitionHostsProvider = partitionHostsProvider;
        this.callerThreads = callerThreads;
    }

    @Override
    public PartitionClient getPartition(PartitionName partitionName) throws Exception {

        return cache.computeIfAbsent(partitionName, (key) -> {
            try {
                HttpPartitionCallRouter partitionCallRouter = new HttpPartitionCallRouter(callerThreads, partitionHostsProvider, null);
                return new AmzaHttpPartitionClient(key, partitionCallRouter);
            } catch (Exception x) {
                throw new RuntimeException(x);
            }
        });
    }

}
