package com.jivesoftware.os.amza.sync.deployable;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class AmzaSyncReceiver {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final PartitionClientProvider partitionClientProvider;

    private final Map<PartitionName, Consistency> consistencyCache = Maps.newConcurrentMap();

    private final long additionalSolverAfterNMillis = 10_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 60_000; //TODO expose to conf?

    public AmzaSyncReceiver(PartitionClientProvider partitionClientProvider) {
        this.partitionClientProvider = partitionClientProvider;
    }

    public void commitRows(PartitionName partitionName, List<Row> rows) throws Exception {
        LOG.info("Received from partition:{} rows:{}", partitionName, rows.size());
        PartitionClient client = partitionClientProvider.getPartition(partitionName);

        Consistency consistency = consistencyCache.computeIfAbsent(partitionName, partitionName1 -> {
            try {
                PartitionProperties properties = partitionClientProvider.getProperties(partitionName1);
                return properties != null ? properties.consistency : null;
            } catch (Exception e) {
                LOG.error("Failed to get properties for partition:{}", partitionName1);
                return null;
            }
        });

        if (consistency == null) {
            throw new RuntimeException("Missing consistency for partition: " + partitionName);
        }

        PeekingIterator<Row> iter = Iterators.peekingIterator(rows.iterator());
        while (iter.hasNext()) {
            byte[] prefix = iter.peek().prefix;
            client.commit(consistency, prefix,
                commitKeyValueStream -> {
                    while (iter.hasNext()) {
                        byte[] peek = iter.peek().prefix;
                        if ((prefix == null && peek == null) || (prefix != null && peek != null && Arrays.equals(prefix, peek))) {
                            Row row = iter.next();
                            commitKeyValueStream.commit(row.key, row.value, row.valueTimestamp, row.valueTombstoned);
                        } else {
                            break;
                        }
                    }
                    return true;
                },
                additionalSolverAfterNMillis,
                abandonSolutionAfterNMillis,
                Optional.empty());
        }
    }

    public void ensurePartition(PartitionName partitionName, int ringSize, PartitionProperties partitionProperties) throws Exception {
        partitionClientProvider.getPartition(partitionName, ringSize, partitionProperties);
    }
}
