package com.jivesoftware.os.amza.lsm.pointers;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.pointers.api.NextPointer;
import com.jivesoftware.os.amza.lsm.pointers.api.PointerIndex;
import com.jivesoftware.os.amza.lsm.pointers.api.PointerStream;
import java.io.File;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LSMPointerIndexEnvironmentNGTest {

    @Test
    public void testSomeMethod() throws Exception {

        File root = Files.createTempDir();
        LSMPointerIndexEnvironment env = new LSMPointerIndexEnvironment(root);

        PointerIndex index = env.open("foo", 1000);

        AtomicLong version = new AtomicLong();
        AtomicLong pointer = new AtomicLong();
        AtomicLong count = new AtomicLong();


        int totalCardinality = 100_000_000;
        int commitCount = 10;
        int batchCount = 100_000;
        int getCount = 10_000;

        Random rand = new Random(12345);
        for (int c = 0; c < commitCount; c++) {
            long start = System.currentTimeMillis();
            index.append((PointerStream stream) -> {
                for (int i = 0; i < batchCount; i++) {
                    count.incrementAndGet();
                    stream.stream(UIO.longBytes(rand.nextInt(totalCardinality)),
                        System.currentTimeMillis(),
                        rand.nextBoolean(),
                        version.incrementAndGet(),
                        pointer.incrementAndGet());
                }
                return true;
            });

            System.out.println("Append Elapse:" + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            index.commit();
            System.out.println("Commit Elapse:" + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();

            AtomicLong hits = new AtomicLong();
            for (int i = 0; i < getCount; i++) {
                index.getPointer(UIO.longBytes(rand.nextInt(1_000_000)), (NextPointer nextPointer) -> {
                    nextPointer.next((byte[] key, long timestamp, boolean tombstoned, long version1, long pointer1) -> {
                        hits.incrementAndGet();
                        return true;
                    });
                    return null;
                });
            }
            System.out.println("Get (" + hits.get() + ") Elapse:" + (System.currentTimeMillis() - start));

            System.out.println("Count:" + count.get());
            System.out.println("-----------------------------------");

        }
    }

}
