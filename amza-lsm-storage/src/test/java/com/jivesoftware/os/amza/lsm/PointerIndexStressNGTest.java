package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.NextPointer;
import java.io.File;
import java.text.NumberFormat;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableLong;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class PointerIndexStressNGTest {

    NumberFormat format = NumberFormat.getInstance();

    @Test(enabled = false)
    public void stress() throws Exception {
        Random rand = new Random();

        long start = System.currentTimeMillis();
        MergeablePointerIndexs indexs = new MergeablePointerIndexs();
        int maxDepthBeforeMerging = 1;
        int count = 0;

        int numBatches = 10;
        int batchSize = 1_000_000;
        int maxKeyIncrement = 10;

        int maxLeaps = (int) (Math.log(numBatches * batchSize) / Math.log(2));
        int updatesBetweenLeaps = 100;

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
                        //File mergeKeysFile = File.createTempFile("d-keys-merged-" + merge.intValue(), ".tmp");

                        return new DiskBackedLeapPointerIndex(
                            new DiskBackedPointerIndexFiler(mergeIndexFiler.getAbsolutePath(), "rw", true), maxLeaps, updatesBetweenLeaps); //,
                        //new DiskBackedPointerIndexFiler(mergeKeysFile.getAbsolutePath(), "rw", false)
                        //);
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
            long getStart = System.currentTimeMillis();
            while (stopGets.longValue() > System.currentTimeMillis()) {

                try {
                    if (running.isTrue() || merging.isTrue()) {
                        Thread.sleep(100);
                        continue;
                    }

                    int longKey = rand.nextInt(maxKey.intValue());
                    byte[] keyBytes = UIO.longBytes(longKey);
                    NextPointer pointer = indexs.getPointer(keyBytes);
                    pointer.next((key, timestamp, tombstoned, version, pointer1) -> {
                        if (pointer1 != -1) {
                            hits[0]++;
                        } else {
                            misses[0]++;
                        }
                        return true;
                    });
                    if ((hits[0] + misses[0]) % 1_000 == 0) {
                        long getEnd = System.currentTimeMillis();
                        System.out.println("Hits:" + hits[0] + " Misses:" + misses[0] + " Elapse:" + (getEnd - getStart));
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
            //File keysFile = File.createTempFile("s-keys-merged-" + b, ".tmp");

            //MemoryPointerIndex index = new MemoryPointerIndex();
            long startMerge = System.currentTimeMillis();

            DiskBackedLeapPointerIndex index = new DiskBackedLeapPointerIndex(
                new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", true), maxLeaps, updatesBetweenLeaps);//,
            //new DiskBackedPointerIndexFiler(keysFile.getAbsolutePath(), "rw", false));

            long lastKey = PointerIndexUtils.append(rand, index, 0, maxKeyIncrement, batchSize, null);
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

}
