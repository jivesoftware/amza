package com.jivesoftware.os.amzabot.deployable;

import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.client.test.InMemoryPartitionClient;
import com.jivesoftware.os.amzabot.deployable.bot.AmzaBotRandomOpConfig;
import com.jivesoftware.os.amzabot.deployable.bot.AmzaBotRandomOpService;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.mlogger.core.AtomicCounter;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AmzaBotRandomOpTest {

    private AmzaBotRandomOpService service;
    private AmzaKeyClearingHouse amzaKeyClearingHouse;
    private AmzaBotRandomOpConfig config;

    @BeforeMethod
    public void setUp() throws Exception {
        OrderIdProvider orderIdProvider = new OrderIdProviderImpl(
            new ConstantWriterIdProvider(1));

        Map<PartitionName, PartitionClient> indexes = new ConcurrentHashMap<>();
        PartitionClientProvider partitionClientProvider = new PartitionClientProvider() {
            @Override
            public PartitionClient getPartition(PartitionName partitionName) throws Exception {
                return indexes.computeIfAbsent(partitionName,
                    partitionName1 -> new InMemoryPartitionClient(ringMember, transactions, new ConcurrentSkipListMap<>(KeyUtil.lexicographicalComparator()), orderIdProvider));
            }

            @Override
            public PartitionClient getPartition(PartitionName partitionName, int desiredRingSize, PartitionProperties partitionProperties) throws Exception {
                return getPartition(partitionName);
            }

            @Override
            public PartitionProperties getProperties(PartitionName partitionName) throws Exception {
                return null;
            }
        };

        AmzaBotConfig amzaBotConfig = BindInterfaceToConfiguration.bindDefault(AmzaBotConfig.class);

        config = BindInterfaceToConfiguration.bindDefault(AmzaBotRandomOpConfig.class);
        config.setEnabled(false);
        config.setHesitationFactorMs(100);
        config.setWriteThreshold(100L);
        config.setValueSizeThreshold(20);
        config.setDurability("fsync_async");
        config.setConsistency("leader_quorum");
        config.setRingSize(1);
        config.setRetryWaitMs(100);
        config.setSnapshotFrequency(10);
        config.setClientOrdering(false);
        config.setBatchFactor(100);

        amzaKeyClearingHouse = new AmzaKeyClearingHouse();
        service = new AmzaBotRandomOpService(
            config,
            new AmzaBotService(amzaBotConfig,
                partitionClientProvider,
                () -> -1L,
                Durability.valueOf(config.getDurability()),
                Consistency.valueOf(config.getConsistency()),
                "amzabot-randomop-test",
                config.getRingSize()),
            amzaKeyClearingHouse);
        service.start();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        service.stop();
    }

    @Test
    public void testWaitOnProcessor() throws InterruptedException {
        Thread.sleep(1_000);

        System.out.println("Outstanding key count: " +
            amzaKeyClearingHouse.getKeyMap().size());

        Assert.assertEquals(amzaKeyClearingHouse.getQuarantinedKeyMap().size(), 0);

        service.clearKeyMap();
        service.clearQuarantinedKeyMap();

        // success
    }

    @Test
    public void testCRUDRandomOp() throws Exception {
        boolean create, read, update, delete, batchWrite, batchDelete;
        create = read = update = delete = batchWrite = batchDelete = false;

        AtomicCounter seq = new AtomicCounter(ValueType.COUNT);

        while (!create || !read || !update || !delete || !batchWrite || !batchDelete) {
            int op = service.randomOp("test:" + String.valueOf(seq.getValue()));

            if (op == 0) {
                read = true;
            } else if (op == 1) {
                delete = true;
            } else if (op == 2) {
                batchWrite = true;
            } else if (op == 3) {
                batchDelete = true;
            } else if (op == 4) {
                update = true;
            } else {
                create = true;
            }

            seq.inc();
        }

        Assert.assertEquals(amzaKeyClearingHouse.getQuarantinedKeyMap().size(), 0);

        service.clearKeyMap();
        service.clearQuarantinedKeyMap();

        // success
    }

    @Test
    public void testMultipleRandomOps() throws Exception {
        AtomicCounter seq = new AtomicCounter(ValueType.COUNT);
        for (int i = 0; i < 10; i++) {
            service.randomOp("test:" + String.valueOf(seq.getValue()));

            if (config.getHesitationFactorMs() > 0) {
                Thread.sleep(new Random().nextInt(config.getHesitationFactorMs()));
            }

            seq.inc();
        }

        System.out.println("Outstanding key count: " +
            amzaKeyClearingHouse.getKeyMap().size());

        Assert.assertEquals(amzaKeyClearingHouse.getQuarantinedKeyMap().size(), 0);

        service.clearKeyMap();
        service.clearQuarantinedKeyMap();

        // success
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void validateInvalidRandomRange() throws Exception {
        Assert.assertEquals(new Random().nextInt(0), 0);
    }

    @Test
    public void validateRandomRange() throws Exception {
        Assert.assertEquals(new Random().nextInt(1), 0);
    }

}
