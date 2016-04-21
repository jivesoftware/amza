package com.jivesoftware.os.amza.service.storage.binary;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.MemoryWALIndex;
import com.jivesoftware.os.amza.api.wal.MemoryWALIndexProvider;
import com.jivesoftware.os.amza.api.wal.MemoryWALUpdates;
import com.jivesoftware.os.amza.api.wal.WALIndexProvider;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALRow;
import com.jivesoftware.os.amza.service.SickPartitions;
import com.jivesoftware.os.amza.service.filer.HeapByteBufferFactory;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.stats.IoStats;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class RowPartitionNGTest {

    final BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller();
    final BinaryHighwaterRowMarshaller highwaterRowMarshaller = new BinaryHighwaterRowMarshaller(new BAInterner());

    @Test(enabled = false)
    public void concurrencyTest() throws Exception {
        File walDir = Files.createTempDir();
        IoStats ioStats = new IoStats();
        RowIOProvider binaryRowIOProvider = new BinaryRowIOProvider(ioStats, 4096, 64, false);

        final WALIndexProvider<MemoryWALIndex> indexProvider = new MemoryWALIndexProvider("memory");
        VersionedPartitionName partitionName = new VersionedPartitionName(new PartitionName(false, "ring".getBytes(), "booya".getBytes()),
            VersionedPartitionName.STATIC_VERSION);

        BinaryWALTx binaryWALTx = new BinaryWALTx(walDir, "booya", binaryRowIOProvider, primaryRowMarshaller, 4096, 64);

        OrderIdProviderImpl idProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        WALStorage<MemoryWALIndex> indexedWAL = new WALStorage<>(
            new AmzaStats(),
            partitionName,
            idProvider,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            binaryWALTx,
            indexProvider,
            new SickPartitions(),
            false,
            2);

        indexedWAL.load(-1, -1, false, false, 0);

        final Random r = new Random();

        ScheduledExecutorService compact = Executors.newScheduledThreadPool(1);
        compact.scheduleAtFixedRate(() -> {
            try {
                indexedWAL.compactTombstone(RowType.primary, 0, 0, Long.MAX_VALUE, Long.MAX_VALUE, 0, false, () -> {
                    return null;
                });
            } catch (Exception x) {
                x.printStackTrace();
            }
        }, 1, 1, TimeUnit.SECONDS);

        int numThreads = 1;
        ExecutorService writers = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            writers.submit(() -> {
                for (int i1 = 1; i1 < 1_000; i1++) {
                    try {
                        addBatch(r, idProvider, indexedWAL, i1, 0, 10);
                        if (i1 % 1000 == 0) {
                            System.out.println(Thread.currentThread() + " batch:" + i1);
                        }
                    } catch (Throwable x) {
                        x.printStackTrace();
                    }
                }
            });
        }

        writers.shutdown();
        writers.awaitTermination(1, TimeUnit.DAYS);

        compact.shutdownNow();

//        addBatch(r, idProvider, table, 10, 0, 10);
//        table.compactTombstone(0);
//        addBatch(r, idProvider, table, 10, 10, 20);
//        table.compactTombstone(0);
//
//        table.rowScan(new RowScan<Exception>() {
//
//            @Override
//            public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws Exception {
//                System.out.println(UIO.bytesInt(key.getKey()));
//                return true;
//            }
//        });
    }

    private void addBatch(Random r, OrderIdProviderImpl idProvider, WALStorage indexedWAL, int range, int start, int length) throws Exception {
        List<WALRow> updates = Lists.newArrayList();
        byte[] prefix = UIO.intBytes(-1);
        for (int i = start; i < start + length; i++) {
            byte[] key = UIO.intBytes(r.nextInt(range));
            byte[] value = UIO.intBytes(i);
            long timestampAndVersion = idProvider.nextId();
            updates.add(new WALRow(RowType.primary, prefix, key, value, timestampAndVersion, false, timestampAndVersion));
        }
        indexedWAL.update(true, RowType.primary, -1, false, prefix, new MemoryWALUpdates(updates, null));
    }

    @Test
    public void diskBackedEventualConsistencyTest() throws Exception {
        File walDir = Files.createTempDir();
        IoStats ioStats = new IoStats();

        RowIOProvider binaryRowIOProvider = new BinaryRowIOProvider(ioStats, 4096, 64, false);

        WALIndexProvider<MemoryWALIndex> indexProvider = new MemoryWALIndexProvider("memory");
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "ring".getBytes(), "booya".getBytes()),
            VersionedPartitionName.STATIC_VERSION);

        BinaryWALTx binaryWALTx = new BinaryWALTx(walDir, "booya", binaryRowIOProvider, primaryRowMarshaller, 4096, 64);

        OrderIdProviderImpl idProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        testEventualConsistency(versionedPartitionName, idProvider, binaryWALTx, indexProvider);
    }

    @Test
    public void memoryBackedEventualConsistencyTest() throws Exception {
        IoStats ioStats = new IoStats();

        RowIOProvider binaryRowIOProvider = new MemoryBackedRowIOProvider(ioStats, 4_096, 4_096, 4_096, 64, new HeapByteBufferFactory());

        WALIndexProvider<MemoryWALIndex> indexProvider = new MemoryWALIndexProvider("memory");
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "ring".getBytes(), "booya".getBytes()),
            VersionedPartitionName.STATIC_VERSION);

        BinaryWALTx binaryWALTx = new BinaryWALTx(null, "booya", binaryRowIOProvider, primaryRowMarshaller, 4096, 64);

        OrderIdProviderImpl idProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        testEventualConsistency(versionedPartitionName, idProvider, binaryWALTx, indexProvider);
    }

    private void testEventualConsistency(VersionedPartitionName versionedPartitionName,
        OrderIdProviderImpl idProvider,
        BinaryWALTx binaryWALTx,
        WALIndexProvider<MemoryWALIndex> walIndexProvider)
        throws Exception {
        WALStorage<MemoryWALIndex> indexedWAL = new WALStorage<>(
            new AmzaStats(),
            versionedPartitionName,
            idProvider,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            binaryWALTx,
            walIndexProvider,
            new SickPartitions(),
            false,
            2);

        indexedWAL.load(-1, -1, false, false, 0);
        WALKey walKey = k(1);
        TimestampedValue value = indexedWAL.getTimestampedValue(walKey.prefix, walKey.key);
        Assert.assertNull(value);

        int t = 10;
        update(indexedWAL, walKey.prefix, walKey.key, v("hello"), t, false);

        value = indexedWAL.getTimestampedValue(walKey.prefix, walKey.key);
        Assert.assertEquals(value.getTimestampId(), t);
        Assert.assertEquals(new String(value.getValue()), "hello");

        t++;
        update(indexedWAL, walKey.prefix, walKey.key, v("hello2"), t, false);
        value = indexedWAL.getTimestampedValue(walKey.prefix, walKey.key);
        Assert.assertEquals(value.getTimestampId(), t);
        Assert.assertEquals(new String(value.getValue()), "hello2");

        update(indexedWAL, walKey.prefix, walKey.key, v("hello3"), t, false);
        value = indexedWAL.getTimestampedValue(walKey.prefix, walKey.key);
        Assert.assertEquals(value.getTimestampId(), t);
        Assert.assertEquals(new String(value.getValue()), "hello2");

        update(indexedWAL, walKey.prefix, walKey.key, v("fail"), t - 1, false);
        value = indexedWAL.getTimestampedValue(walKey.prefix, walKey.key);
        Assert.assertEquals(value.getTimestampId(), t);
        Assert.assertEquals(new String(value.getValue()), "hello2");

        t++;
        update(indexedWAL, walKey.prefix, walKey.key, v("deleted"), t, false);
        value = indexedWAL.getTimestampedValue(walKey.prefix, walKey.key);
        Assert.assertEquals(value.getTimestampId(), t);
        Assert.assertEquals(new String(value.getValue()), "deleted");

        update(indexedWAL, walKey.prefix, walKey.key, v("fail"), t - 1, false);
        value = indexedWAL.getTimestampedValue(walKey.prefix, walKey.key);
        Assert.assertEquals(value.getTimestampId(), t);
        Assert.assertEquals(new String(value.getValue()), "deleted");

        t++;
        update(indexedWAL, walKey.prefix, walKey.key, v("hello4"), t, false);
        value = indexedWAL.getTimestampedValue(walKey.prefix, walKey.key);
        Assert.assertEquals(value.getTimestampId(), t);
        Assert.assertEquals(new String(value.getValue()), "hello4");
    }

    public WALKey k(int key) {
        return new WALKey(UIO.intBytes(-key), UIO.intBytes(key));
    }

    public byte[] v(String value) {
        return value.getBytes();
    }

    private void update(WALStorage indexedWAL, byte[] prefix, byte[] key, byte[] value, long timestamp, boolean remove) throws Exception {
        List<WALRow> updates = Lists.newArrayList();
        updates.add(new WALRow(RowType.primary, prefix, key, value, timestamp, remove, timestamp));
        indexedWAL.update(true, RowType.primary, -1, false, prefix, new MemoryWALUpdates(updates, null));
    }
}
