package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.GetRaw;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntryStream;
import java.io.File;
import java.text.NumberFormat;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableLong;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class IndexStressNGTest {

    NumberFormat format = NumberFormat.getInstance();

    @Test(enabled = true)
    public void stress() throws Exception {
        Random rand = new Random();

        long start = System.currentTimeMillis();
        MergeableIndexes indexs = new MergeableIndexes();
        int maxDepthBeforeMerging = 1;
        int count = 0;

        int numBatches = 10;
        int batchSize = 1_000_000;
        int maxKeyIncrement = 10;

        int maxLeaps = (int) (Math.log(numBatches * batchSize) / Math.log(2));
        int updatesBetweenLeaps = 1024;

        MutableLong merge = new MutableLong();
        MutableLong maxKey = new MutableLong();
        MutableBoolean running = new MutableBoolean(true);
        MutableBoolean merging = new MutableBoolean(true);
        MutableLong stopGets = new MutableLong(Long.MAX_VALUE);
        Future<Object> merger = Executors.newSingleThreadExecutor().submit(() -> {
            while ((running.isTrue() || indexs.mergeDebt() > 1)
                || (indexs.mergeDebt() > maxDepthBeforeMerging)) {

                try {
                    long startMerge = System.currentTimeMillis();
                    if (indexs.merge(maxDepthBeforeMerging, () -> {

                        merge.increment();
                        File mergeIndexFiler = File.createTempFile("d-index-merged-" + merge.intValue(), ".tmp");

                        return new LeapsAndBoundsIndex(
                            new IndexFile(mergeIndexFiler.getAbsolutePath(), "rw", true), maxLeaps, updatesBetweenLeaps);
                    }, (index) -> {
                        System.out.println("Commit Merged index:" + index);
                        return index;
                    })) {
                        System.out.println("Merge (" + merge.intValue() + ") elapse:" + format.format((System.currentTimeMillis() - startMerge)) + "millis");
                    } else {
                        //System.out.println("Nothing to merge. Sleeping. "+indexs.mergeDebut());
                        Thread.sleep(10);
                    }
                } catch (Exception x) {
                    x.printStackTrace();
                    Thread.sleep(10_000);
                }
            }

            //stopGets.setValue(System.currentTimeMillis() + 60_000);
            return null;

        });

        Future<Object> pointGets = Executors.newSingleThreadExecutor().submit(() -> {

            int[] hits = {0};
            int[] misses = {0};
            RawEntryStream hitsAndMisses = (rawEntry, offset, length) -> {
                if (rawEntry != null) {
                    hits[0]++;
                } else {
                    misses[0]++;
                }
                return true;
            };
            long getStart = System.currentTimeMillis();
            long best = Long.MAX_VALUE;
            long total = 0;
            long samples = 0;
            byte[] key = new byte[8];
            while (stopGets.longValue() > System.currentTimeMillis()) {

                try {
                    if (running.isTrue() || merging.isTrue()) {
                        Thread.sleep(100);
                        continue;
                    }

                    GetRaw pointer = indexs.get();
                    int longKey = rand.nextInt(maxKey.intValue());
                    UIO.longBytes(longKey, key, 0);
                    pointer.get(key, hitsAndMisses);
                    int logInterval = 100_000;
                    if ((hits[0] + misses[0]) % logInterval == 0) {
                        long getEnd = System.currentTimeMillis();
                        long elapse = (getEnd - getStart);
                        total += elapse;
                        samples++;
                        if (elapse < best) {
                            best = elapse;
                        }

                        System.out.println(
                            "Hits:" + hits[0] + " Misses:" + misses[0] + " Elapse:" + elapse + " Best:" + rps(logInterval, best) + " Avg:" + rps(logInterval,
                            (long) ((double) total / (double) samples)));
                        hits[0] = 0;
                        misses[0] = 0;
                        getStart = getEnd;
                    }
                    //Thread.sleep(1);
                } catch (Exception x) {
                    x.printStackTrace();
                    Thread.sleep(10);
                }
            }
            return null;

        });

        for (int b = 0; b < numBatches; b++) {

            File indexFiler = File.createTempFile("s-index-merged-" + b, ".tmp");

            //MemoryPointerIndex index = new MemoryPointerIndex();
            long startMerge = System.currentTimeMillis();

            LeapsAndBoundsIndex index = new LeapsAndBoundsIndex(new IndexFile(indexFiler.getAbsolutePath(), "rw", true), maxLeaps, updatesBetweenLeaps);

            long lastKey = IndexTestUtils.append(rand, index, 0, maxKeyIncrement, batchSize, null);
            maxKey.setValue(Math.max(maxKey.longValue(), lastKey));
            indexs.append(index);
            count += batchSize;

            System.out.println("Insertions:" + format.format(count) + " ips:" + format.format(
                ((count / (double) (System.currentTimeMillis() - start))) * 1000) + " elapse:" + format.format(
                    (System.currentTimeMillis() - startMerge)) + " mergeDebut:" + indexs.mergeDebt());
        }

        running.setValue(false);
        merger.get();
        /*System.out.println("Sleeping 10 sec before gets...");
        Thread.sleep(10_000L);*/
        merging.setValue(false);
        pointGets.get();

        System.out.println("Done. " + (System.currentTimeMillis() - start));

    }

    private  long rps(long logInterval, long elapse) {
        return (long) ((logInterval / (double) elapse) * 1000);
    }

}
