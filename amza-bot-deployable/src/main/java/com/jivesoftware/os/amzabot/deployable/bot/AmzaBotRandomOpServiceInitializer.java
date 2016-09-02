package com.jivesoftware.os.amzabot.deployable.bot;

import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amzabot.deployable.AmzaBotConfig;
import com.jivesoftware.os.amzabot.deployable.AmzaBotService;
import com.jivesoftware.os.amzabot.deployable.AmzaKeyClearingHouse;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class AmzaBotRandomOpServiceInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaBotConfig amzaBotConfig;
    private final AmzaBotRandomOpConfig amzaBotRandomOpConfig;
    private final PartitionClientProvider partitionClientProvider;
    private final AmzaKeyClearingHouse amzaKeyClearingHouse;

    public AmzaBotRandomOpServiceInitializer(AmzaBotConfig amzaBotConfig,
        AmzaBotRandomOpConfig amzaBotRandomOpConfig,
        PartitionClientProvider partitionClientProvider,
        AmzaKeyClearingHouse amzaKeyClearingHouse) {
        this.amzaBotConfig = amzaBotConfig;
        this.amzaBotRandomOpConfig = amzaBotRandomOpConfig;
        this.partitionClientProvider = partitionClientProvider;
        this.amzaKeyClearingHouse = amzaKeyClearingHouse;
    }

    public AmzaBotRandomOpService initialize() throws Exception {
        LOG.info("Hesitation factor {}", amzaBotRandomOpConfig.getHesitationFactorMs());
        LOG.info("Write threshold {}", amzaBotRandomOpConfig.getWriteThreshold());
        LOG.info("Value size threshold {}", amzaBotRandomOpConfig.getValueSizeThreshold());
        LOG.info("Durability {}", amzaBotRandomOpConfig.getDurability());
        LOG.info("Consistency {}", amzaBotRandomOpConfig.getConsistency());
        LOG.info("Partition size {}", amzaBotRandomOpConfig.getPartitionSize());
        LOG.info("Retry wait {}ms", amzaBotRandomOpConfig.getRetryWaitMs());
        LOG.info("Snapshot frequency {}", amzaBotRandomOpConfig.getSnapshotFrequency());

        PartitionProperties partitionProperties = new PartitionProperties(
            Durability.valueOf(amzaBotRandomOpConfig.getDurability()),
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            false,
            Consistency.valueOf(amzaBotRandomOpConfig.getConsistency()),
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
            ("amzabot-randomops-" + UUID.randomUUID().toString()).getBytes(StandardCharsets.UTF_8));

        PartitionClient partitionClient = partitionClientProvider.getPartition(
            partitionName,
            amzaBotRandomOpConfig.getPartitionSize(),
            partitionProperties);
        LOG.info("Created partition for random operations {}", partitionName);

        return new AmzaBotRandomOpService(amzaBotRandomOpConfig,
            new AmzaBotService(amzaBotConfig, partitionClient, Consistency.leader_quorum),
            amzaKeyClearingHouse);
    }

}
