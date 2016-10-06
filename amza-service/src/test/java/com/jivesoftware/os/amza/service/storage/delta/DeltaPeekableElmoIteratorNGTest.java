package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.stream.FpKeyValueStream;
import com.jivesoftware.os.amza.api.stream.Fps;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALPointer;
import com.jivesoftware.os.amza.api.wal.WALRow;
import com.jivesoftware.os.amza.api.wal.WALValue;
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
            ConcurrentSkipListMap<byte[], WALPointer> wal = new ConcurrentSkipListMap<>(KeyUtil::compare);
            ConcurrentSkipListMap<byte[], WALPointer> other = new ConcurrentSkipListMap<>(KeyUtil::compare);
            Random rand = new Random();
            NavigableSet<Byte> expected = new TreeSet<>();
            NavigableSet<Byte> expectedBoth = new TreeSet<>();
            final Map<Long, WALRow> fpRows = Maps.newHashMap();
            for (int i = 0; i < 128; i++) {
                if (rand.nextBoolean()) {
                    long timestamp = rand.nextInt(128);
                    byte[] prefix = new byte[]{(byte) i};
                    byte[] key = new byte[]{(byte) i};
                    WALPointer pointer = new WALPointer((long) i, timestamp, false, timestamp, false, null);
                    WALValue value = new WALValue(RowType.primary, UIO.longBytes((long) i), timestamp, false, timestamp);
                    wal.put(WALKey.compose(prefix, key), pointer);
                    fpRows.put((long) i, new WALRow(RowType.primary, prefix, key, value.getValue(), value.getTimestampId(), value.getTombstoned(), value
                        .getVersion()));
                    expected.add((byte) i);
                    expectedBoth.add((byte) i);
                }
                if (rand.nextBoolean()) {
                    long timestamp = rand.nextInt(128);
                    byte[] prefix = new byte[]{(byte) i};
                    byte[] key = new byte[]{(byte) i};
                    WALPointer pointer = new WALPointer((long) i, timestamp, false, timestamp, false, null);
                    WALValue value = new WALValue(RowType.primary, UIO.longBytes((long) i), timestamp, false, timestamp);
                    other.put(WALKey.compose(prefix, key), pointer);
                    fpRows.put((long) i, new WALRow(RowType.primary, prefix, key, value.getValue(), value.getTimestampId(), value.getTombstoned(), value
                        .getVersion()));
                    expectedBoth.add((byte) i);
                }
            }

            WALRowHydrator hydrator = new WALRowHydrator() {
                @Override
                public boolean hydrate(Fps fps, FpKeyValueStream fpKeyValueStream) throws Exception {
                    return fps.consume(fp -> {
                        WALRow row = fpRows.get(fp);
                        return fpKeyValueStream.stream(fp, row.rowType, row.prefix, row.key, row.value, row.timestamp, row.tombstoned, row.version);
                    });
                }

                @Override
                public WALValue hydrate(long fp) throws Exception {
                    WALRow row = fpRows.get(fp);
                    return new WALValue(row.rowType, row.value, row.timestamp, row.tombstoned, row.version);
                }

                @Override
                public void closeHydrator() {
                }
            };
            DeltaPeekableElmoIterator deltaPeekableElmoIterator = new DeltaPeekableElmoIterator(
                wal.entrySet().iterator(),
                Iterators.<Map.Entry<byte[], WALPointer>>emptyIterator(),
                hydrator,
                hydrator,
                true);

            List<Byte> had = new ArrayList<>();
            long[] lastV = {-1};
            while (deltaPeekableElmoIterator.hasNext()) {
                byte[] pk = deltaPeekableElmoIterator.next().getKey();
                WALKey.decompose(txFpRawKeyValueEntryStream -> txFpRawKeyValueEntryStream.stream(-1, -1, null, pk, false, null, -1, false, -1, null),
                    (txId, fp, rowType, prefix, key, hasValue, value, valueTimestamp, valueTombstoned, valueVersion, entry) -> {
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
                hydrator,
                true);

            had.clear();
            lastV[0] = -1;
            while (deltaPeekableElmoIterator.hasNext()) {
                byte[] pk = deltaPeekableElmoIterator.next().getKey();
                WALKey.decompose(txFpRawKeyValueEntryStream -> txFpRawKeyValueEntryStream.stream(-1, -1, null, pk, false, null, -1, false, -1, null),
                    (txId, fp, rowType, prefix, key, hasValue, value, valueTimestamp, valueTombstoned, valueVersion, entry) -> {
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
