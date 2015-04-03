package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Iterators;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
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
            NavigableMap<WALKey, WALValue> wal = new ConcurrentSkipListMap<>();
            NavigableMap<WALKey, WALValue> other = new ConcurrentSkipListMap<>();
            Random rand = new Random();
            Set<Byte> expected = new HashSet<>();
            Set<Byte> expectedBoth = new HashSet<>();
            for (int i = 0; i < 128; i++) {
                if (rand.nextBoolean()) {
                    wal.put(new WALKey(new byte[]{(byte) i}), new WALValue(new byte[]{(byte) i}, rand.nextInt(128), false));
                    expected.add((byte) i);
                    expectedBoth.add((byte) i);
                }
                if (rand.nextBoolean()) {
                    other.put(new WALKey(new byte[]{(byte) i}), new WALValue(new byte[]{(byte) i}, rand.nextInt(128), false));
                    expectedBoth.add((byte) i);
                }
            }

            DeltaPeekableElmoIterator deltaPeekableElmoIterator = new DeltaPeekableElmoIterator(wal.entrySet().iterator(),
                Iterators.<Map.Entry<WALKey, WALValue>>emptyIterator());

            Set<Byte> had = new HashSet<>();
            long lastV = -1;
            while (deltaPeekableElmoIterator.hasNext()) {
                byte v = deltaPeekableElmoIterator.next().getKey().getKey()[0];
                had.add(v);
                Assert.assertTrue(lastV < v);
                lastV = v;
            }
            Assert.assertEquals(had.size(), expected.size());

            deltaPeekableElmoIterator = new DeltaPeekableElmoIterator(wal.entrySet().iterator(),
                other.entrySet().iterator());

            had = new HashSet<>();
            lastV = -1;
            while (deltaPeekableElmoIterator.hasNext()) {
                byte v = deltaPeekableElmoIterator.next().getKey().getKey()[0];
                had.add(v);
                Assert.assertTrue(lastV < v);
                lastV = v;
            }
            Assert.assertEquals(had.size(), expectedBoth.size());
        }
    }

}
