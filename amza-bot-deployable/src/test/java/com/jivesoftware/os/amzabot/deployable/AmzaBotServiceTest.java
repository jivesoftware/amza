package com.jivesoftware.os.amzabot.deployable;

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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AmzaBotServiceTest {

    AmzaBotService amzaBotService;

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

        AmzaBotConfig amzaBotConfig = BindInterfaceToConfiguration.bindDefault(AmzaBotConfig.class);
        amzaBotConfig.setDropEverythingOnTheFloor(false);
        amzaBotConfig.setAdditionalSolverAfterNMillis(10_000);
        amzaBotConfig.setAbandonSolutionAfterNMillis(30_000);

        amzaBotService = new AmzaBotService(amzaBotConfig, partitionClientProvider);
    }

    @Test
    public void testBotSet() throws Exception {
        for (long i = 0; i < 10; i++) {
            amzaBotService.set("key:" + i, "value:" + i);
        }

        for (long i = 0; i < 10; i++) {
            String v = amzaBotService.get("key:" + i);

            if (v.isEmpty()) {
                System.out.println("Error:" + i);
            } else {
                System.out.println(v);
            }
        }
    }

    @Test
    public void testBotDelete() throws Exception {
        for (long i = 0; i < 10; i++) {
            amzaBotService.set("key:" + i, "value:" + i);
        }

        for (long i = 0; i < 10; i++) {
            amzaBotService.delete("key:" + i);
        }
    }

}
