package com.jivesoftware.os.amza.service.storage.delta;

import com.google.common.collect.Lists;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class OverConsumingEntryIteratorNGTest {

    @Test
    public void testSimple() {
        List<Map.Entry<Byte, Integer>> entries = new ArrayList<>();
        List<Map.Entry<Byte, Integer>> expected = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                entries.add(new AbstractMap.SimpleEntry<>((byte) i, j));
            }
            expected.add(new AbstractMap.SimpleEntry<>((byte) i, 9));
        }
        OverConsumingEntryIterator<Byte, Integer> iterator = new OverConsumingEntryIterator<>(entries.iterator());
        List<Map.Entry<Byte, Integer>> consumed = Lists.newArrayList(iterator);
        assertEquals(consumed, expected);
    }
}