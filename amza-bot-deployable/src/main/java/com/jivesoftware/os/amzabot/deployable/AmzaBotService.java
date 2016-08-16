package com.jivesoftware.os.amzabot.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public class AmzaBotService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaBotConfig config;
    private final PartitionClientProvider clientProvider;

    private final PartitionProperties AMZABOT_PROPERTIES = new PartitionProperties(
        Durability.fsync_async,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
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

    private final PartitionName partitionName = new PartitionName(false,
        ("amzabot").getBytes(StandardCharsets.UTF_8),
        ("amzabot").getBytes(StandardCharsets.UTF_8));

    public AmzaBotService(AmzaBotConfig config, PartitionClientProvider clientProvider) {
        this.config = config;
        this.clientProvider = clientProvider;
    }

    public void set(String k, String v) throws Exception {
        LOG.warn("set {} - {}", k, v);

        if (config.getDropEverythingOnTheFloor()) {
            LOG.warn("Dropping sets on the floor.");
            return;
        }

        PartitionClient partitionClient = clientProvider.getPartition(
            partitionName,
            3,
            AMZABOT_PROPERTIES);

        partitionClient.commit(
            Consistency.leader_quorum,
            null,
            (commitKeyValueStream) -> {
                commitKeyValueStream.commit(k.getBytes(), v.getBytes(), -1, false);
                return true;
            },
            config.getAdditionalSolverAfterNMillis(),
            config.getAbandonSolutionAfterNMillis(),
            Optional.empty());
    }

    public String get(String k) throws Exception {
        LOG.warn("get {}", k);

        PartitionClient partitionClient = clientProvider.getPartition(
            partitionName,
            3,
            AMZABOT_PROPERTIES);

        List<String> values = Lists.newArrayList();
        partitionClient.get(Consistency.leader_quorum,
            null,
            (keyStream) -> keyStream.stream(k.getBytes(StandardCharsets.UTF_8)),
            (prefix, key, value, timestamp, version) -> {
                values.add(new String(value, StandardCharsets.UTF_8));
                return true;
            },
            config.getAbandonSolutionAfterNMillis(),
            config.getAdditionalSolverAfterNMillis(),
            config.getAbandonSolutionAfterNMillis(),
            Optional.empty());

        String v = "";
        if (values.size() == 1) {
            v = values.get(0);
        } else {
            LOG.warn("key {} returned {}", k, values);
        }
        return v;
    }

    public void delete(String k) throws Exception {
        LOG.warn("delete {}", k);

        PartitionClient partitionClient = clientProvider.getPartition(
            partitionName,
            3,
            AMZABOT_PROPERTIES);

        partitionClient.commit(
            Consistency.leader_quorum,
            null,
            (commitKeyValueStream) -> {
                commitKeyValueStream.commit(k.getBytes(), null, -1, true);
                return true;
            },
            config.getAdditionalSolverAfterNMillis(),
            config.getAbandonSolutionAfterNMillis(),
            Optional.empty());
    }

}
