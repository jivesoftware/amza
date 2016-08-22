package com.jivesoftware.os.amzabot.deployable;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.client.test.InMemoryPartitionClient;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.mlogger.core.AtomicCounter;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AmzaBotServiceTest {

    private AmzaBotService amzaBotService;
    private AmzaKeyClearingHouse amzaKeyClearingHouse;
    private AmzaBotConfig amzaBotConfig;


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
        amzaBotConfig.setDropEverythingOnTheFloor(false);
        amzaBotConfig.setAdditionalSolverAfterNMillis(10_000);
        amzaBotConfig.setAbandonSolutionAfterNMillis(30_000);
        amzaBotConfig.setHesitationFactorMs(100);
        amzaBotConfig.setWriteThreshold(100);
        amzaBotConfig.setValueSizeThreshold(20);

        amzaKeyClearingHouse = new AmzaKeyClearingHouse();
        amzaBotService = new AmzaBotService(amzaBotConfig, partitionClientProvider, amzaKeyClearingHouse);
        amzaBotService.start();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        amzaBotService.stop();
    }

    @Test
    public void testBotSet() throws Exception {
        for (long i = 0; i < 10; i++) {
            amzaBotService.set("key:" + i, "value:" + i);
        }

        for (long i = 0; i < 10; i++) {
            String v = amzaBotService.get("key:" + i);

            Assert.assertNotNull(v);
            Assert.assertEquals(v, "value:" + i);
        }

        amzaBotService.clearQuarantinedKeys();
    }

    @Test
    public void testBotDelete() throws Exception {
        for (long i = 0; i < 10; i++) {
            amzaBotService.set("key:" + i, "value:" + i);
        }

        for (long i = 0; i < 10; i++) {
            amzaBotService.delete("key:" + i);
        }

        amzaBotService.clearQuarantinedKeys();

        // success
    }

    @Test
    public void testGetEndpointJoinUsage() throws Exception {
        List<String> noValues = Lists.newArrayList();
        Assert.assertEquals(noValues.size(), 0);

        String noValue = Joiner.on(',').join(noValues);
        Assert.assertEquals(noValue, "");

        List<String> oneValues = Lists.newArrayList();
        oneValues.add("one");
        String oneValue = Joiner.on(',').join(oneValues);
        Assert.assertEquals(oneValue, "one");

        List<String> twoValues = Lists.newArrayList();
        twoValues.add("one");
        twoValues.add("two");
        String twoValue = Joiner.on(',').join(twoValues);
        Assert.assertEquals(twoValue, "one,two");
    }

    @Test
    public void testWaitOnProcessor() throws InterruptedException {
        Thread.sleep(1_000);

        System.out.println("Outstanding key count: " +
            amzaKeyClearingHouse.getKeyMap().size());

        Assert.assertEquals(amzaKeyClearingHouse.getQuarantinedKeyMap().size(), 0);

        amzaBotService.clearQuarantinedKeys();
    }

    @Test
    public void testWRDRandomOp() throws Exception {
        boolean write, read, delete;
        write = read = delete = false;

        AtomicCounter seq = new AtomicCounter(ValueType.COUNT);

        while (!write || !read || !delete) {
            int op = amzaBotService.randomOp("test:" + String.valueOf(seq.getValue()));

            if (op == 0) {
                read = true;
            } else if (op == 1) {
                delete = true;
            } else {
                write = true;
            }

            seq.inc();
        }

        amzaBotService.clearQuarantinedKeys();
    }

    @Test
    public void testMultipleRandomOps() throws Exception {
        AtomicCounter seq = new AtomicCounter(ValueType.COUNT);
        for (int i = 0; i < 10; i++) {
            amzaBotService.randomOp("test:" + String.valueOf(seq.getValue()));
            Thread.sleep(amzaBotConfig.getHesitationFactorMs());

            seq.inc();
        }

        System.out.println("Outstanding key count: " +
            amzaKeyClearingHouse.getKeyMap().size());

        Assert.assertEquals(amzaKeyClearingHouse.getQuarantinedKeyMap().size(), 0);

        amzaBotService.clearQuarantinedKeys();
    }

}
