package com.jivesoftware.os.amza.sync.deployable;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.RingPartitionProperties;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class AmzaSyncReceiver {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final PartitionClientProvider partitionClientProvider;
    private final boolean useSolutionLog;

    private final Map<PartitionName, Consistency> consistencyCache = Maps.newConcurrentMap();

    private final long additionalSolverAfterNMillis = 10_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 60_000; //TODO expose to conf?

    public AmzaSyncReceiver(PartitionClientProvider partitionClientProvider, boolean useSolutionLog) {
        this.partitionClientProvider = partitionClientProvider;
        this.useSolutionLog = useSolutionLog;
    }

    public void commitRows(PartitionName partitionName, List<Row> rows) throws Exception {
        LOG.info("Received from partition:{} rows:{}", partitionName, rows.size());
        PartitionClient client = partitionClientProvider.getPartition(partitionName);

        Consistency consistency = consistencyCache.computeIfAbsent(partitionName, partitionName1 -> {
            try {
                RingPartitionProperties properties = partitionClientProvider.getProperties(partitionName1);
                return properties != null ? properties.partitionProperties.consistency : null;
            } catch (Exception e) {
                LOG.error("Failed to get properties for partition:{}", partitionName1);
                return null;
            }
        });

        if (consistency == null) {
            throw new RuntimeException("Missing consistency for partition: " + partitionName);
        }

        PeekingIterator<Row> iter = Iterators.peekingIterator(rows.iterator());
        Optional<List<String>> solutionLog = useSolutionLog ? Optional.of(Collections.synchronizedList(Lists.newArrayList())) : Optional.empty();
        int[] batch = { 0 };
        try {
            while (iter.hasNext()) {
                batch[0]++;
                solutionLog.ifPresent(log -> log.add("Batch " + batch[0]));
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
                    solutionLog);
            }
        } catch (Exception e) {
            if (solutionLog.isPresent()) {
                LOG.error("Commit failure for {}, solution log:\n - {}", partitionName, Joiner.on("\n - ").join(solutionLog.get()));
            }
            throw e;
        }
    }

    public void ensurePartition(PartitionName partitionName, int ringSize, PartitionProperties partitionProperties) throws Exception {
        partitionClientProvider.getPartition(partitionName, ringSize, partitionProperties);
    }
}
