package com.jivesoftware.os.amzabot.deployable.bot;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amzabot.deployable.AmzaBotConfig;
import com.jivesoftware.os.amzabot.deployable.AmzaBotService;
import com.jivesoftware.os.amzabot.deployable.AmzaKeyClearingHousePool;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AmzaBotCoalmineService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaBotConfig amzaBotConfig;
    private final AmzaBotCoalmineConfig amzaBotCoalmineConfig;
    private final PartitionClientProvider partitionClientProvider;
    private final AmzaKeyClearingHousePool amzaKeyClearingHousePool;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService processor =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("amzabot-coalmine-%d").build());

    public AmzaBotCoalmineService(AmzaBotConfig amzaBotConfig,
        AmzaBotCoalmineConfig amzaBotCoalmineConfig,
        PartitionClientProvider partitionClientProvider,
        AmzaKeyClearingHousePool amzaKeyClearingHousePool) {
        this.amzaBotConfig = amzaBotConfig;
        this.amzaBotCoalmineConfig = amzaBotCoalmineConfig;
        this.partitionClientProvider = partitionClientProvider;
        this.amzaKeyClearingHousePool = amzaKeyClearingHousePool;
    }

    public AmzaBotCoalminer newMinerWithConfig(AmzaBotCoalmineConfig config) throws Exception {
        LOG.info("Coalmine capacity {}", config.getCoalmineCapacity());
        LOG.info("Canary size threshold {}", config.getCanarySizeThreshold());
        LOG.info("Hesitation {}ms", config.getHesitationMs());
        LOG.info("Durability {}", config.getDurability());
        LOG.info("Consistency {}", config.getConsistency());
        LOG.info("Partition size {}", config.getPartitionSize());

        PartitionProperties partitionProperties = new PartitionProperties(
            Durability.valueOf(config.getDurability()),
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            false,
            Consistency.valueOf(config.getConsistency()),
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
            ("amzabot-coalmine-" + UUID.randomUUID().toString()).getBytes(StandardCharsets.UTF_8));

        PartitionClient partitionClient = partitionClientProvider.getPartition(
            partitionName,
            config.getPartitionSize(),
            partitionProperties);
        LOG.info("Created partition for coalmine {}", partitionName);

        return new AmzaBotCoalminer(
            config,
            new AmzaBotService(amzaBotConfig,
                partitionClient,
                Consistency.valueOf(config.getConsistency())),
            amzaKeyClearingHousePool);
    }

    public AmzaBotCoalminer newMiner() throws Exception {
        return newMinerWithConfig(amzaBotCoalmineConfig);
    }

    public void start() {
        if (!amzaBotCoalmineConfig.getEnabled()) {
            LOG.warn("Not starting coalminer service; not enabled.");
            return;
        }

        LOG.info("Frequency {}ms", amzaBotCoalmineConfig.getFrequencyMs());

        running.set(true);

        processor.scheduleWithFixedDelay(() -> {
            if (running.get()) {
                LOG.info("Scheduling coalmine thread");

                try {
                    ExecutorService executor = Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder().setNameFormat("amzabot-coalmine-%d").build());
                    executor.submit(newMiner());
                } catch (Exception e) {
                    LOG.error("Error occurred scheduling coalmine.", e);
                }
            }
        }, 0, amzaBotCoalmineConfig.getFrequencyMs(), TimeUnit.MILLISECONDS);
    }

    public void stop() throws InterruptedException {
        running.set(false);
        Thread.sleep(amzaBotCoalmineConfig.getHesitationMs());

        processor.shutdownNow();
    }

}
