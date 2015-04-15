package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.UIO;
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
 *
 * @author jonathan.colt
 */
public class DeltaPeekableElmoIteratorNGTest {

    @Test
    public void testSimple() {
        for (int r = 0; r < 10; r++) {
            NavigableMap<WALKey, WALPointer> wal = new ConcurrentSkipListMap<>();
            NavigableMap<WALKey, WALPointer> other = new ConcurrentSkipListMap<>();
            WALValueHydrator hydrator = new WALValueHydrator() {
                @Override
                public WALValue hydrate(WALPointer rowPointer) throws Exception {
                    return new WALValue(UIO.longBytes(rowPointer.getFp()), rowPointer.getTimestampId(), rowPointer.getTombstoned());
                }
            };
            Random rand = new Random();
            NavigableSet<Byte> expected = new TreeSet<>();
            NavigableSet<Byte> expectedBoth = new TreeSet<>();
            for (int i = 0; i < 128; i++) {
                if (rand.nextBoolean()) {
                    wal.put(new WALKey(new byte[]{(byte) i}), new WALPointer((long) i, rand.nextInt(128), false));
                    expected.add((byte) i);
                    expectedBoth.add((byte) i);
                }
                if (rand.nextBoolean()) {
                    other.put(new WALKey(new byte[]{(byte) i}), new WALPointer((long) i, rand.nextInt(128), false));
                    expectedBoth.add((byte) i);
                }
            }

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
