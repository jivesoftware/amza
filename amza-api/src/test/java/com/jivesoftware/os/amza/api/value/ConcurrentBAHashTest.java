package com.jivesoftware.os.amza.api.value;

import com.beust.jcommander.internal.Sets;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class ConcurrentBAHashTest {

    @Test
    public void testStream() throws Exception {
        ConcurrentBAHash<String> concurrentBAHash = new ConcurrentBAHash<>(10, true, 4);
        Set<String> expected = Sets.newHashSet();
        for (int i = 0; i < 1_000; i++) {
            String k = String.valueOf(i);
            concurrentBAHash.put(k.getBytes(), k);
            expected.add(k);
        }

        Set<String> actual = Sets.newHashSet();
        concurrentBAHash.stream((key, value) -> {
            actual.add(value);
            return true;
        });

        assertEquals(actual, expected);
    }
}