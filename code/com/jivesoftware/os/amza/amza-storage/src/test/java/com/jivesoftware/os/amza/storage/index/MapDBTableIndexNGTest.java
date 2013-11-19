package com.jivesoftware.os.amza.storage.index;

import com.jivesoftware.os.amza.shared.BasicTimestampedValue;
import org.testng.annotations.Test;

public class MapDBTableIndexNGTest {

    @Test (enabled = false)
    public void loadTest() {
        System.out.println("InitialHeap:" + Runtime.getRuntime().totalMemory());
        MapDBTableIndex<Long, Long> instance = new MapDBTableIndex<>("test");
        for (long i = 0; i < 200000000; i++) {
            instance.put(i, new BasicTimestampedValue<>(i, i, false));
            if (i % 1000000 == 0) {
                System.out.println("Size:" + i + " Heap:" + Runtime.getRuntime().totalMemory());
                instance.flush();
            }
        }
        instance.flush();

        System.out.println("Heap:" + Runtime.getRuntime().totalMemory());
        // TODO review the generated test code and remove the default call to fail.
    }
}