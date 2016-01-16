package com.jivesoftware.os.amza.service.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionStripeFunction;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.IndexedWALStorageProvider;
import com.jivesoftware.os.amza.service.SickPartitions;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.filer.HeapByteBufferFactory;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.stats.IoStats;
import com.jivesoftware.os.amza.service.storage.JacksonPartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.service.storage.binary.MemoryBackedRowIOProvider;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import java.io.File;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class AmzaStateStorageNGTest {

    private final BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller();
    private final BinaryHighwaterRowMarshaller highwaterRowMarshaller = new BinaryHighwaterRowMarshaller();

    @Test
    public void testUpdate() throws Exception {

        OrderIdProviderImpl ids = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        ObjectMapper mapper = new ObjectMapper();
        JacksonPartitionPropertyMarshaller partitionPropertyMarshaller = new JacksonPartitionPropertyMarshaller(mapper);

        File partitionTmpDir = Files.createTempDir();
        File[] workingDirectories = {partitionTmpDir};
        IoStats ioStats = new IoStats();
        MemoryBackedRowIOProvider ephemeralRowIOProvider = new MemoryBackedRowIOProvider(ioStats,
            1_024,
            1_024 * 1_024,
            4_096,
            64,
            new HeapByteBufferFactory());
        BinaryRowIOProvider persistentRowIOProvider = new BinaryRowIOProvider(
            ioStats,
            4_096,
            64,
            false);
        WALIndexProviderRegistry walIndexProviderRegistry = new WALIndexProviderRegistry(ephemeralRowIOProvider, persistentRowIOProvider);

        IndexedWALStorageProvider indexedWALStorageProvider = new IndexedWALStorageProvider(new PartitionStripeFunction(workingDirectories.length),
            workingDirectories,
            walIndexProviderRegistry, primaryRowMarshaller, highwaterRowMarshaller, ids, new SickPartitions(), -1);

        TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1), new SnowflakeIdPacker(),
            new JiveEpochTimestampProvider());
        AmzaStats amzaStats = new AmzaStats();
        PartitionIndex partitionIndex = new PartitionIndex(amzaStats,
            orderIdProvider,
            indexedWALStorageProvider,
            partitionPropertyMarshaller);

        partitionIndex.open();

        SystemWALStorage systemWALStorage = new SystemWALStorage(partitionIndex,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            null,
            false);

        WALUpdated updated = (versionedPartitionName, txId) -> {
        };

        Member root = new Member(new byte[]{1});
        Member other1 = new Member(new byte[]{2});
        Member other2 = new Member(new byte[]{3});

        PartitionName partitionName = new PartitionName(false, new byte[]{20}, new byte[]{30});
        byte context = 1;
        AmzaStateStorage stateStorage = new AmzaStateStorage(systemWALStorage, updated, root, partitionName, context);

        Long lifecycle1 = 1L;
        Long lifecycle2 = 2L;

        stateStorage.update((setLiveliness) -> {

            for (Long lifecycle : new Long[]{lifecycle1, lifecycle2}) {
                setLiveliness.set(root, root, lifecycle, State.leader, 1);
                setLiveliness.set(root, other1, lifecycle, State.follower, 1);
                setLiveliness.set(root, other2, lifecycle, State.follower, 1);

                setLiveliness.set(other1, root, lifecycle, State.leader, 1);
                setLiveliness.set(other1, other1, lifecycle, State.follower, 1);
                setLiveliness.set(other1, other2, lifecycle, State.follower, 1);

                setLiveliness.set(other2, root, lifecycle, State.leader, 1);
                setLiveliness.set(other2, other1, lifecycle, State.follower, 1);
                setLiveliness.set(other2, other2, lifecycle, State.follower, 1);
            }
            return true;
        });

        System.out.println("--------------------------");

        int[] count = new int[1];
        for (Long lifecycle : new Long[]{lifecycle1, lifecycle2}) {
            for (Member m : new Member[]{root, other1, other2}) {
                stateStorage.scan(m, null, lifecycle, (rootMember, isSelf, ackMember, alifecycle, state, timestamp, version) -> {

                    Assert.assertEquals(lifecycle, alifecycle);
                    Assert.assertEquals(m, rootMember);
                    System.out.println(rootMember + " " + isSelf + " " + ackMember + " " + alifecycle + " " + state + " " + timestamp + " " + version);
                    count[0]++;
                    return true;
                });
                Assert.assertEquals(count[0], 3);
                count[0] = 0;
            }
        }

        stateStorage.scan(null, null, null, (rootMember, isSelf, ackMember, alifecycle, state, timestamp, version) -> {

            System.out.println(rootMember + " " + isSelf + " " + ackMember + " " + alifecycle + " " + state + " " + timestamp + " " + version);
            count[0]++;
            return true;
        });
        Assert.assertEquals(count[0], 18);
        count[0] = 0;

    }

}
