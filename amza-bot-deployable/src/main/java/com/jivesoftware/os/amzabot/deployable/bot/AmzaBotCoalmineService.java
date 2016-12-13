package com.jivesoftware.os.amzabot.deployable.bot;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amzabot.deployable.AmzaBotConfig;
import com.jivesoftware.os.amzabot.deployable.AmzaBotService;
import com.jivesoftware.os.amzabot.deployable.AmzaKeyClearingHousePool;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AmzaBotCoalmineService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final InstanceConfig instanceConfig;
    private final AmzaBotConfig amzaBotConfig;
    private final AmzaBotCoalmineConfig amzaBotCoalmineConfig;
    private final PartitionClientProvider partitionClientProvider;
    private final AmzaKeyClearingHousePool amzaKeyClearingHousePool;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService processor =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("amzabot-coalmine-%d").build());

    public AmzaBotCoalmineService(InstanceConfig instanceConfig,
        AmzaBotConfig amzaBotConfig,
        AmzaBotCoalmineConfig amzaBotCoalmineConfig,
        PartitionClientProvider partitionClientProvider,
        AmzaKeyClearingHousePool amzaKeyClearingHousePool) {
        this.instanceConfig = instanceConfig;
        this.amzaBotConfig = amzaBotConfig;
        this.amzaBotCoalmineConfig = amzaBotCoalmineConfig;
        this.partitionClientProvider = partitionClientProvider;
        this.amzaKeyClearingHousePool = amzaKeyClearingHousePool;
    }

    public AmzaBotCoalminer newMinerWithConfig(AmzaBotCoalmineConfig config) throws Exception {
        LOG.info("Coalmine capacity: {}", config.getCoalmineCapacity());
        LOG.info("Canary size threshold: {}", config.getCanarySizeThreshold());
        LOG.info("Hesitation: {}ms", config.getHesitationMs());
        LOG.info("Durability: {}", config.getDurability());
        LOG.info("Consistency: {}", config.getConsistency());
        LOG.info("Ring size: {}", config.getRingSize());
        LOG.info("Client timestamp: {}", config.getClientOrdering());

        OrderIdProvider orderIdProvider = () -> -1L;
        if (config.getClientOrdering()) {
            orderIdProvider = new OrderIdProviderImpl(
                new ConstantWriterIdProvider(instanceConfig.getInstanceName()));
        }

        return new AmzaBotCoalminer(
            config,
            new AmzaBotService(amzaBotConfig,
                partitionClientProvider,
                orderIdProvider,
                Durability.valueOf(config.getDurability()),
                Consistency.valueOf(config.getConsistency()),
                "amzabot-coalmine-" + UUID.randomUUID().toString(),
                config.getRingSize(),
                0,
                0),
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
