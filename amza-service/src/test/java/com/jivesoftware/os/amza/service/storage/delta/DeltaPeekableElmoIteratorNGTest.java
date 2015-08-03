package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.wal.FpKeyValueHighwaterStream;
import com.jivesoftware.os.amza.shared.wal.FpKeyValueStream;
import com.jivesoftware.os.amza.shared.wal.Fps;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALPointer;
import com.jivesoftware.os.amza.shared.wal.WALRow;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class DeltaPeekableElmoIteratorNGTest {

    @Test
    public void testSimple() throws Exception {
        for (int r = 0; r < 10; r++) {
            ConcurrentSkipListMap<byte[], WALPointer> wal = new ConcurrentSkipListMap<>(WALKey::compare);
            ConcurrentSkipListMap<byte[], WALPointer> other = new ConcurrentSkipListMap<>(WALKey::compare);
            Random rand = new Random();
            NavigableSet<Byte> expected = new TreeSet<>();
            NavigableSet<Byte> expectedBoth = new TreeSet<>();
            final Map<Long, WALRow> fpRows = Maps.newHashMap();
            for (int i = 0; i < 128; i++) {
                if (rand.nextBoolean()) {
                    long timestamp = rand.nextInt(128);
                    byte[] prefix = new byte[] { (byte) i };
                    byte[] key = new byte[] { (byte) i };
                    WALPointer pointer = new WALPointer((long) i, timestamp, false);
                    WALValue value = new WALValue(UIO.longBytes((long) i), timestamp, false);
                    wal.put(WALKey.compose(prefix, key), pointer);
                    fpRows.put((long) i, new WALRow(prefix, key, value.getValue(), value.getTimestampId(), value.getTombstoned()));
                    expected.add((byte) i);
                    expectedBoth.add((byte) i);
                }
                if (rand.nextBoolean()) {
                    long timestamp = rand.nextInt(128);
                    byte[] prefix = new byte[] { (byte) i };
                    byte[] key = new byte[] { (byte) i };
                    WALPointer pointer = new WALPointer((long) i, timestamp, false);
                    WALValue value = new WALValue(UIO.longBytes((long) i), timestamp, false);
                    other.put(WALKey.compose(prefix, key), pointer);
                    fpRows.put((long) i, new WALRow(prefix, key, value.getValue(), value.getTimestampId(), value.getTombstoned()));
                    expectedBoth.add((byte) i);
                }
            }

            WALRowHydrator hydrator = new WALRowHydrator() {
                @Override
                public boolean hydrate(Fps fps, FpKeyValueStream fpKeyValueStream) throws Exception {
                    return fps.consume(fp -> {
                        WALRow row = fpRows.get(fp);
                        return fpKeyValueStream.stream(fp, row.prefix, row.key, row.value, row.timestamp, row.tombstoned);
                    });
                }

                @Override
                public WALValue hydrate(long fp) throws Exception {
                    WALRow row = fpRows.get(fp);
                    return new WALValue(row.value, row.timestamp, row.tombstoned);
                }
            };
            DeltaPeekableElmoIterator deltaPeekableElmoIterator = new DeltaPeekableElmoIterator(
                wal.entrySet().iterator(),
                Iterators.<Map.Entry<byte[], WALPointer>>emptyIterator(),
                hydrator,
                hydrator);

            List<Byte> had = new ArrayList<>();
            long[] lastV = { -1 };
            while (deltaPeekableElmoIterator.hasNext()) {
                byte[] pk = deltaPeekableElmoIterator.next().getKey();
                WALKey.decompose(txFpRawKeyValueEntryStream -> txFpRawKeyValueEntryStream.stream(-1, -1, pk, null, -1, false, null),
                    (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, entry) -> {
                        byte v = key[0];
                        had.add(v);
                        Assert.assertTrue(lastV[0] < v);
                        lastV[0] = v;
                        return true;
                    });
            }
            Assert.assertEquals(had, Lists.newArrayList(expected));

            deltaPeekableElmoIterator = new DeltaPeekableElmoIterator(
                wal.entrySet().iterator(),
                other.entrySet().iterator(),
                hydrator,
                hydrator);

            had.clear();
            lastV[0] = -1;
            while (deltaPeekableElmoIterator.hasNext()) {
                byte[] pk = deltaPeekableElmoIterator.next().getKey();
                WALKey.decompose(txFpRawKeyValueEntryStream -> txFpRawKeyValueEntryStream.stream(-1, -1, pk, null, -1, false, null),
                    (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, entry) -> {
                        byte v = key[0];
                        had.add(v);
                        Assert.assertTrue(lastV[0] < v);
                        lastV[0] = v;
                        return true;
                    });
            }
            Assert.assertEquals(had, Lists.newArrayList(expectedBoth));
        }
    }

}
