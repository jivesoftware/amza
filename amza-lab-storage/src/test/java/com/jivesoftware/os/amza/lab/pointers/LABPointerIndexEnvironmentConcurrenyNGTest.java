package com.jivesoftware.os.amza.lab.pointers;

import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexEnvironment;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lab.pointers.api.NextPointer;
import com.jivesoftware.os.amza.lab.pointers.api.PointerIndex;
import com.jivesoftware.os.amza.lab.pointers.api.PointerStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABPointerIndexEnvironmentConcurrenyNGTest {

    @Test(enabled = false)
    public void testSomeMethod() throws Exception {

        File root = Files.createTempDir();
        LABPointerIndexEnvironment env = new LABPointerIndexEnvironment(root, 2);

        int writerCount = 16;
        int readerCount = 16;

        AtomicLong hits = new AtomicLong();
        AtomicLong version = new AtomicLong();
        AtomicLong pointer = new AtomicLong();
        AtomicLong count = new AtomicLong();

        int totalCardinality = 100_000_000;
        int commitCount = 34;
        int batchCount = 3_000;

        ExecutorService writers = Executors.newFixedThreadPool(writerCount);
        ExecutorService readers = Executors.newFixedThreadPool(readerCount);

        Random rand = new Random(12345);
        PointerIndex index = env.open("foo", 1000);
        AtomicLong running = new AtomicLong();
        List<Future> writerFutures = new ArrayList<>();
        for (int i = 0; i < writerCount; i++) {
            running.incrementAndGet();
            writerFutures.add(writers.submit(() -> {
                try {
                    for (int c = 0; c < commitCount; c++) {
                        index.append((PointerStream stream) -> {
                            for (int b = 0; b < batchCount; b++) {
                                count.incrementAndGet();
                                stream.stream(UIO.longBytes(rand.nextInt(totalCardinality)),
                                    System.currentTimeMillis(),
                                    rand.nextBoolean(),
                                    version.incrementAndGet(),
                                    pointer.incrementAndGet());
                            }
                            return true;
                        });
                        index.commit();
                        System.out.println(" gets:" + hits.get());
                    }
                    return null;
                } catch (Exception x) {
                    x.printStackTrace();
                    throw x;
                } finally {
                    running.decrementAndGet();
                }
            }));
        }

        List<Future> readerFutures = new ArrayList<>();
        for (int r = 0; r < readerCount; r++) {
            int readerId = r;
            readerFutures.add(readers.submit(() -> {
                try {
                    while (running.get() > 0) {
                        index.getPointer(UIO.longBytes(rand.nextInt(1_000_000)), (NextPointer nextPointer) -> {
                            nextPointer.next((byte[] key, long timestamp, boolean tombstoned, long version1, long pointer1) -> {
                                hits.incrementAndGet();
                                return true;
                            });
                            return null;
                        });
                    }
                    System.out.println("Reader (" + readerId + ") finished.");
                    return null;
                } catch (Exception x) {
                    x.printStackTrace();
                    throw x;
                }
            }));
        }

        for (Future f : writerFutures) {
            f.get();
        }

        for (Future f : readerFutures) {
            f.get();
        }

        System.out.println("ALL DONE");
        System.out.println("ALL DONE");
        System.out.println("ALL DONE");
        System.out.println("ALL DONE");
        System.out.println("ALL DONE");
        System.out.println("ALL DONE");

        writers.shutdownNow();
        readers.shutdownNow();

        //Thread.sleep(Integer.MAX_VALUE);

    }

}
