package com.jivesoftware.os.amza.service.storage.binary;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.amza.service.storage.filer.MemoryBackedWALFiler;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.shared.wal.MemoryWALIndex;
import com.jivesoftware.os.amza.shared.wal.MemoryWALIndexProvider;
import com.jivesoftware.os.amza.shared.wal.MemoryWALUpdates;
import com.jivesoftware.os.amza.shared.wal.WALIndexProvider;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALRow;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.io.File;
import java.nio.ByteBuffer;
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
    final BinaryHighwaterRowMarshaller highwaterRowMarshaller = new BinaryHighwaterRowMarshaller();

    @Test(enabled = false)
    public void concurrencyTest() throws Exception {
        File walDir = Files.createTempDir();
        //RowIOProvider binaryRowIOProvider = new BufferedBinaryRowIOProvider();
        IoStats ioStats = new IoStats();
        RowIOProvider<File> binaryRowIOProvider = new BinaryRowIOProvider(new String[] { walDir.getAbsolutePath() }, ioStats, 1, false);

        final WALIndexProvider<MemoryWALIndex> indexProvider = new MemoryWALIndexProvider();
        VersionedPartitionName partitionName = new VersionedPartitionName(new PartitionName(false, "ring".getBytes(), "booya".getBytes()),
            VersionedPartitionName.STATIC_VERSION);

        BinaryWALTx<MemoryWALIndex, ?> binaryWALTx = new BinaryWALTx<>(walDir, "booya", binaryRowIOProvider, primaryRowMarshaller, indexProvider);

        OrderIdProviderImpl idProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        WALStorage<MemoryWALIndex> indexedWAL = new WALStorage<>(
            partitionName,
            idProvider,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            binaryWALTx,
            1000,
            1000,
            2);

        indexedWAL.load();

        final Random r = new Random();

        ScheduledExecutorService compact = Executors.newScheduledThreadPool(1);
        compact.scheduleAtFixedRate(() -> {
            try {
                indexedWAL.compactTombstone(0, Long.MAX_VALUE, false);
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
            updates.add(new WALRow(prefix, key, value, timestampAndVersion, false, timestampAndVersion));
        }
        indexedWAL.update(-1, false, prefix, new MemoryWALUpdates(updates, null));
    }

    @Test
    public void diskBackedEventualConsistencyTest() throws Exception {
        File walDir = Files.createTempDir();
        IoStats ioStats = new IoStats();

        RowIOProvider<File> binaryRowIOProvider = new BinaryRowIOProvider(new String[] { walDir.getAbsolutePath() }, ioStats, 1, false);

        WALIndexProvider<MemoryWALIndex> indexProvider = new MemoryWALIndexProvider();
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "ring".getBytes(), "booya".getBytes()),
            VersionedPartitionName.STATIC_VERSION);

        BinaryWALTx<MemoryWALIndex, ?> binaryWALTx = new BinaryWALTx<>(walDir, "booya", binaryRowIOProvider, primaryRowMarshaller, indexProvider);

        OrderIdProviderImpl idProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        testEventualConsitency(versionedPartitionName, idProvider, binaryWALTx);
    }

    @Test
    public void memoryBackedEventualConsistencyTest() throws Exception {
        File walDir = Files.createTempDir();
        IoStats ioStats = new IoStats();

        RowIOProvider<File> binaryRowIOProvider = new MemoryBackedRowIOProvider(new String[] { walDir.getAbsolutePath() }, ioStats, 1, 4_096, 4_096,
            size -> ByteBuffer.allocate(size.intValue()));

        WALIndexProvider<MemoryWALIndex> indexProvider = new MemoryWALIndexProvider();
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(new PartitionName(false, "ring".getBytes(), "booya".getBytes()),
            VersionedPartitionName.STATIC_VERSION);

        BinaryWALTx<MemoryWALIndex, ?> binaryWALTx = new BinaryWALTx<>(null, "booya", binaryRowIOProvider, primaryRowMarshaller, indexProvider);

        OrderIdProviderImpl idProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
        testEventualConsitency(versionedPartitionName, idProvider, binaryWALTx);
    }

    private void testEventualConsitency(VersionedPartitionName versionedPartitionName,
        OrderIdProviderImpl idProvider,
        BinaryWALTx<MemoryWALIndex, ?> binaryWALTx)
        throws Exception {
        WALStorage<MemoryWALIndex> indexedWAL = new WALStorage<>(
            versionedPartitionName,
            idProvider,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            binaryWALTx,
            1000,
            1000,
            2);

        indexedWAL.load();
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
        updates.add(new WALRow(prefix, key, value, timestamp, remove, timestamp));
        indexedWAL.update(-1, false, prefix, new MemoryWALUpdates(updates, null));
    }
}
