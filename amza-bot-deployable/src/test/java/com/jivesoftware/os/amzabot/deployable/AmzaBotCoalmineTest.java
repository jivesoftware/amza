package com.jivesoftware.os.amzabot.deployable;

import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.client.test.InMemoryPartitionClient;
import com.jivesoftware.os.amzabot.deployable.bot.AmzaBotCoalmineConfig;
import com.jivesoftware.os.amzabot.deployable.bot.AmzaBotCoalmineService;
import com.jivesoftware.os.amzabot.deployable.bot.AmzaBotCoalminer;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AmzaBotCoalmineTest {

    private AmzaBotCoalmineService service;
    private AmzaBotConfig amzaBotConfig;
    private AmzaBotCoalmineConfig amzaBotCoalmineConfig;

    @BeforeMethod
    public void setUp() throws Exception {
        OrderIdProviderImpl orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1),
            new SnowflakeIdPacker(),
            new JiveEpochTimestampProvider());

        Map<PartitionName, PartitionClient> indexes = new ConcurrentHashMap<>();
        PartitionClientProvider partitionClientProvider = new PartitionClientProvider() {
            @Override
            public PartitionClient getPartition(PartitionName partitionName) throws Exception {
                return indexes.computeIfAbsent(partitionName,
                    partitionName1 -> new InMemoryPartitionClient(new ConcurrentSkipListMap<>(KeyUtil.lexicographicalComparator()), orderIdProvider));
            }

            @Override
            public PartitionClient getPartition(PartitionName partitionName, int desiredRingSize, PartitionProperties partitionProperties) throws Exception {
                return getPartition(partitionName);
            }
        };

        amzaBotConfig = BindInterfaceToConfiguration.bindDefault(AmzaBotConfig.class);

        amzaBotCoalmineConfig = BindInterfaceToConfiguration.bindDefault(AmzaBotCoalmineConfig.class);
        amzaBotCoalmineConfig.setEnabled(false);
        amzaBotCoalmineConfig.setFrequencyMs(60_000L);
        amzaBotCoalmineConfig.setCoalmineCapacity(10L);
        amzaBotCoalmineConfig.setCanarySizeThreshold(10);
        amzaBotCoalmineConfig.setHesitationMs(1);
        amzaBotCoalmineConfig.setDurability(String.valueOf(Durability.fsync_async));
        amzaBotCoalmineConfig.setConsistency(String.valueOf(Consistency.leader_quorum));
        amzaBotCoalmineConfig.setRingSize(1);

        service = new AmzaBotCoalmineService(
            amzaBotConfig,
            amzaBotCoalmineConfig,
            partitionClientProvider,
            new AmzaKeyClearingHousePool());
        service.start();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        service.stop();
    }

    @Test
    public void testOneMiner() throws Exception {
        AmzaBotCoalminer amzaBotCoalminer = service.newMiner();
        amzaBotCoalminer.run();
    }

    @Test
    public void testWaitOnCoalminers() throws Exception {
        Thread.sleep(1_000L);
    }

}
