package com.jivesoftware.os.amza.client.aquarium;

import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.RingPartitionProperties;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.client.test.InMemoryPartitionClient;
import com.jivesoftware.os.aquarium.AquariumStats;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import org.apache.commons.lang.ArrayUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotEquals;

/**
 *
 */
public class AmzaClientAquariumProviderTest {

    @Test(enabled = false, description = "Slow sanity test")
    public void testLeaderFailover() throws Exception {
        final String serviceName = "test";
        final int ringSize = 3;
        final int numAquariums = 3;

        TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(
            new ConstantWriterIdProvider(1),
            new SnowflakeIdPacker(),
            new JiveEpochTimestampProvider());
        Map<PartitionName, PartitionClient> indexes = new ConcurrentHashMap<>();
        PartitionClientProvider partitionClientProvider = new PartitionClientProvider() {
            @Override
            public PartitionClient getPartition(PartitionName partitionName) throws Exception {
                return indexes.computeIfAbsent(partitionName,
                    partitionName1 -> new InMemoryPartitionClient(new RingMember("member1"),
                        new ConcurrentSkipListMap<>(),
                        new ConcurrentSkipListMap<>(KeyUtil.lexicographicalComparator()), orderIdProvider));
            }

            @Override
            public PartitionClient getPartition(PartitionName partitionName, int desiredRingSize, PartitionProperties partitionProperties) throws Exception {
                return getPartition(partitionName);
            }

            @Override
            public RingPartitionProperties getProperties(PartitionName partitionName) throws Exception {
                return null;
            }
        };
        AquariumStats aquariumStats = new AquariumStats();

        AmzaClientAquariumProvider[] providers = new AmzaClientAquariumProvider[ringSize];
        for (int i = 0; i < ringSize; i++) {
            providers[i] = new AmzaClientAquariumProvider(aquariumStats,
                serviceName,
                partitionClientProvider,
                orderIdProvider,
                new Member(("member" + i).getBytes(StandardCharsets.UTF_8)),
                count -> count > ringSize / 2,
                member -> true,
                128,
                128,
                1_000L,
                100L,
                10_000L,
                2_000L,
                Executors.newSingleThreadExecutor(),
                100L,
                1_000L,
                10_000L,
                false);
            providers[i].start();

            for (int j = 0; j < numAquariums; j++) {
                providers[i].register("aquarium" + j);
            }
        }

        boolean[] leaders = monitorAquariums(providers, numAquariums, ringSize, 20_000L, 1_000L);
        int stopIndex = ArrayUtils.indexOf(leaders, true);
        assertNotEquals(stopIndex, -1);

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        providers[stopIndex].stop();
        System.out.println("STOPPED index " + stopIndex);

        leaders = monitorAquariums(providers, numAquariums, ringSize, 40_000L, 1_000L);

        for (int i = 0; i < ringSize; i++) {
            providers[i].stop();
        }
    }

    private boolean[] monitorAquariums(AmzaClientAquariumProvider[] providers, int numAquariums, int ringSize, long duration, long interval) throws Exception {
        boolean[] leaders = new boolean[providers.length];
        long start = System.currentTimeMillis();
        while (true) {
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            System.out.println("t=" + (System.currentTimeMillis() - start));
            for (int j = 0; j < numAquariums; j++) {
                System.out.println("\ta=" + j);
                for (int i = 0; i < ringSize; i++) {
                    LivelyEndState livelyEndState = providers[i].livelyEndState("aquarium" + j);
                    if (livelyEndState == null) {
                        System.out.println("\t\t" + i + ": offline");
                    } else {
                        System.out.println("\t\t" + i + ": " + (livelyEndState.isOnline() ? livelyEndState.getCurrentState() : "dead"));
                        leaders[i] |= livelyEndState.isOnline() && livelyEndState.getCurrentState() == State.leader;
                    }
                }
            }
            if (System.currentTimeMillis() - start > duration) {
                break;
            } else {
                Thread.sleep(interval);
            }
        }
        return leaders;
    }
}