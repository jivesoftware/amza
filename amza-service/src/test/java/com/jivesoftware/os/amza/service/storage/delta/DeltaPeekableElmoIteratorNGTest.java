package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.storage.WALRow;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
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
    public void testSimple() {
        for (int r = 0; r < 10; r++) {
            NavigableMap<WALKey, WALPointer> wal = new ConcurrentSkipListMap<>();
            NavigableMap<WALKey, WALPointer> other = new ConcurrentSkipListMap<>();
            Random rand = new Random();
            NavigableSet<Byte> expected = new TreeSet<>();
            NavigableSet<Byte> expectedBoth = new TreeSet<>();
            final Map<Long, WALRow> fpRows = Maps.newHashMap();
            for (int i = 0; i < 128; i++) {
                if (rand.nextBoolean()) {
                    long timestamp = rand.nextInt(128);
                    WALKey key = new WALKey(new byte[]{(byte) i});
                    WALPointer pointer = new WALPointer((long) i, timestamp, false);
                    WALValue value = new WALValue(UIO.longBytes((long) i), timestamp, false);
                    wal.put(key, pointer);
                    fpRows.put((long) i, new WALRow(key, value));
                    expected.add((byte) i);
                    expectedBoth.add((byte) i);
                }
                if (rand.nextBoolean()) {
                    long timestamp = rand.nextInt(128);
                    WALKey key = new WALKey(new byte[]{(byte) i});
                    WALPointer pointer = new WALPointer((long) i, timestamp, false);
                    WALValue value = new WALValue(UIO.longBytes((long) i), timestamp, false);
                    other.put(key, pointer);
                    fpRows.put((long) i, new WALRow(key, value));
                    expectedBoth.add((byte) i);
                }
            }

            WALRowHydrator hydrator = fpRows::get;
            DeltaPeekableElmoIterator deltaPeekableElmoIterator = new DeltaPeekableElmoIterator(
                wal.entrySet().iterator(),
                Iterators.<Map.Entry<WALKey, WALPointer>>emptyIterator(),
                hydrator,
                hydrator);

            List<Byte> had = new ArrayList<>();
            long lastV = -1;
            while (deltaPeekableElmoIterator.hasNext()) {
                byte v = deltaPeekableElmoIterator.next().getKey().getKey()[0];
                had.add(v);
                Assert.assertTrue(lastV < v);
                lastV = v;
            }
            Assert.assertEquals(had, Lists.newArrayList(expected));

            deltaPeekableElmoIterator = new DeltaPeekableElmoIterator(
                wal.entrySet().iterator(),
                other.entrySet().iterator(),
                hydrator,
                hydrator);

            had = new ArrayList<>();
            lastV = -1;
            while (deltaPeekableElmoIterator.hasNext()) {
                byte v = deltaPeekableElmoIterator.next().getKey().getKey()[0];
                had.add(v);
                Assert.assertTrue(lastV < v);
                lastV = v;
            }
            Assert.assertEquals(had, Lists.newArrayList(expectedBoth));
        }
    }

}
