package com.jivesoftware.os.amzabot.deployable;

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

public class AmzaBotServiceInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaBotConfig config;
    private final PartitionClientProvider partitionClientProvider;

    public AmzaBotServiceInitializer(AmzaBotConfig config,
        PartitionClientProvider partitionClientProvider) {
        this.config = config;
        this.partitionClientProvider = partitionClientProvider;
    }

    public AmzaBotService initialize() throws Exception {
        LOG.info("Await Leader Election {}ms", config.getAmzaAwaitLeaderElectionForNMillis());
        LOG.info("Caller ThreadPool Size {}", config.getAmzaCallerThreadPoolSize());
        LOG.info("Additional Solver After {}ms", config.getAdditionalSolverAfterNMillis());
        LOG.info("Abandon Solution After {}ms", config.getAbandonSolutionAfterNMillis());
        LOG.info("Abandon Leader Solution After {}ms", config.getAbandonLeaderSolutionAfterNMillis());
        LOG.info("Drop Everything On The Floor {}", config.getDropEverythingOnTheFloor());
        LOG.info("Partition Size {}", config.getPartitionSize());

        PartitionProperties partitionProperties = new PartitionProperties(
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

        PartitionName partitionName = new PartitionName(false,
            ("amzabot").getBytes(StandardCharsets.UTF_8),
            ("amzabot-rest").getBytes(StandardCharsets.UTF_8));

        PartitionClient client = partitionClientProvider.getPartition(
            partitionName,
            config.getPartitionSize(),
            partitionProperties);
        LOG.info("Created partition for rest clients {}", partitionName);

        return new AmzaBotService(config, client, Consistency.leader_quorum);
    }

}
