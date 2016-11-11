package com.jivesoftware.os.amzabot.deployable;

import com.beust.jcommander.internal.Sets;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.client.test.InMemoryPartitionClient;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AmzaBotServiceTest {

    private AmzaBotService service;

    @BeforeMethod
    public void setUp() throws Exception {
        OrderIdProvider orderIdProvider = new OrderIdProviderImpl(
            new ConstantWriterIdProvider(1));

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

            @Override
            public PartitionProperties getProperties(PartitionName partitionName) throws Exception {
                return null;
            }
        };

        AmzaBotConfig config = BindInterfaceToConfiguration.bindDefault(AmzaBotConfig.class);
        config.setRingSize(1);

        service = new AmzaBotService(
            config,
            partitionClientProvider,
            () -> -1,
            Durability.fsync_async,
            Consistency.leader_quorum,
            "amzabot-service-test",
            config.getRingSize());
    }

    @Test
    public void testBotSetGet() throws Exception {
        for (long i = 0; i < 10; i++) {
            service.set("key:" + i, "value:" + i);
        }

        for (long i = 0; i < 10; i++) {
            String v = service.get("key:" + i);

            Assert.assertNotNull(v);
            Assert.assertEquals(v, "value:" + i);
        }
    }

    @Test
    public void testBotBatchSetGet() throws Exception {
        Set<Entry<String, String>> entries = Sets.newHashSet();
        for (long i = 0; i < 10; i++) {
            entries.add(new AbstractMap.SimpleEntry<>("key:" + i, "value:" + i));
        }
        service.multiSet(entries);

        for (long i = 0; i < 10; i++) {
            String v = service.get("key:" + i);

            Assert.assertNotNull(v);
            Assert.assertEquals(v, "value:" + i);
        }
    }

    @Test
    public void testBotSetWithNoRetryGet() throws Exception {
        for (long i = 0; i < 10; i++) {
            service.setWithRetry("key:" + i, "value:" + i, 0, 0);
        }

        for (long i = 0; i < 10; i++) {
            String v = service.getWithRetry("key:" + i, 0, 0);

            Assert.assertNotNull(v);
            Assert.assertEquals(v, "value:" + i);
        }
    }

    @Test
    public void testBotSetWithRetryGet() throws Exception {
        for (long i = 0; i < 10; i++) {
            service.setWithRetry("key:" + i, "value:" + i, Integer.MAX_VALUE, 100);
        }

        for (long i = 0; i < 10; i++) {
            String v = service.get("key:" + i);

            Assert.assertNotNull(v);
            Assert.assertEquals(v, "value:" + i);
        }
    }

    @Test
    public void testBotSetWithInfiniteRetryGet() throws Exception {
        for (long i = 0; i < 10; i++) {
            service.setWithInfiniteRetry("key:" + i, "value:" + i, 100);
        }

        for (long i = 0; i < 10; i++) {
            String v = service.get("key:" + i);

            Assert.assertNotNull(v);
            Assert.assertEquals(v, "value:" + i);
        }
    }

    @Test
    public void testBotSetDelete() throws Exception {
        for (long i = 0; i < 10; i++) {
            service.set("key:" + i, "value:" + i);
        }

        for (long i = 0; i < 10; i++) {
            service.delete("key:" + i);
        }

        for (long i = 0; i < 10; i++) {
            String v = service.get("key:" + i);

            Assert.assertNull(v);
        }
    }

    @Test
    public void testBotSetDeleteWithNoRetry() throws Exception {
        for (long i = 0; i < 10; i++) {
            service.set("key:" + i, "value:" + i);
        }

        for (long i = 0; i < 10; i++) {
            service.deleteWithRetry("key:" + i, 0, 0);
        }

        for (long i = 0; i < 10; i++) {
            String v = service.get("key:" + i);

            Assert.assertNull(v);
        }
    }

    @Test
    public void testBotSetDeleteWithInfiniteRetry() throws Exception {
        for (long i = 0; i < 10; i++) {
            service.set("key:" + i, "value:" + i);
        }

        for (long i = 0; i < 10; i++) {
            service.deleteWithInfiniteRetry("key:" + i, 100);
        }

        for (long i = 0; i < 10; i++) {
            String v = service.get("key:" + i);

            Assert.assertNull(v);
        }
    }

    @Test
    public void testBotSetDeleteWithRetry() throws Exception {
        for (long i = 0; i < 10; i++) {
            service.set("key:" + i, "value:" + i);
        }

        for (long i = 0; i < 10; i++) {
            service.deleteWithRetry("key:" + i, Integer.MAX_VALUE, 100);
        }

        for (long i = 0; i < 10; i++) {
            String v = service.get("key:" + i);

            Assert.assertNull(v);
        }
    }

    @Test
    public void testBotSetGetAll() throws Exception {
        for (long i = 0; i < 10; i++) {
            service.set("key:" + i, "value:" + i);
        }

        Map<String, String> all = service.getAll();
        Assert.assertEquals(all.size(), 10);

        for (int i = 0; i < 10; i++) {
            String value = all.remove("key:" + i);
            Assert.assertNotNull(value);
            Assert.assertEquals(value, "value:" + i);
        }

        Assert.assertEquals(all.size(), 0);
    }

    @Test
    public void testBotSetGetAllWithRetry() throws Exception {
        for (long i = 0; i < 10; i++) {
            service.set("key:" + i, "value:" + i);
        }

        Map<String, String> all = service.getAllWithRetry(Integer.MAX_VALUE, 100);
        Assert.assertEquals(all.size(), 10);

        for (int i = 0; i < 10; i++) {
            String value = all.remove("key:" + i);
            Assert.assertNotNull(value);
            Assert.assertEquals(value, "value:" + i);
        }

        Assert.assertEquals(all.size(), 0);
    }

    @Test
    public void testBotSetGetAllWithInfiniteRetry() throws Exception {
        for (long i = 0; i < 10; i++) {
            service.set("key:" + i, "value:" + i);
        }

        Map<String, String> all = service.getAllWithInfiniteRetry(100);
        Assert.assertEquals(all.size(), 10);

        for (int i = 0; i < 10; i++) {
            String value = all.remove("key:" + i);
            Assert.assertNotNull(value);
            Assert.assertEquals(value, "value:" + i);
        }

        Assert.assertEquals(all.size(), 0);
    }

    @Test
    public void emitRoundRobinLogic() throws Exception {
        for (int last = 0; last < 6; last++) {
            int len = 3;
            int[] indexes = new int[len];
            for (int i = 0; i < len; i++) {
                indexes[i] = (last + i + 1) % len;
            }
            System.out.println(Arrays.toString(indexes));
        }
    }

    @Test
    public void verifySbBehavior() throws Exception {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 2; i++) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(i);
            sb.append(":");
            sb.append(i);
        }

        sb.insert(0, "pre: ");
        Assert.assertEquals(sb.toString(), "pre: 0:0,1:1");
    }

}
