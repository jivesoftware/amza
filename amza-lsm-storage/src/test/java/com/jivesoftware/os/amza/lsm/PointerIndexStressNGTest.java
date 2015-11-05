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

    @Test(enabled = true)
    public void stress() throws Exception {
        long start = System.currentTimeMillis();
        MergeablePointerIndexs indexs = new MergeablePointerIndexs();
        int maxDepthBeforeMerging = 2;
        int count = 0;

        int maxLeaps = 64;
        int updatesBetweenLeaps = 4096;

        MutableLong merge = new MutableLong();
        MutableBoolean running = new MutableBoolean(true);
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
                            new DiskBackedPointerIndexFiler(mergeIndexFiler.getAbsolutePath(), "rw", false), maxLeaps, updatesBetweenLeaps); //,
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

            stopGets.setValue(System.currentTimeMillis() + 20_000);
            return null;

        });

        int numBatches = 10;
        int batchSize = 1_000_000;
        int maxKeyIncrement = 2;
        Random rand = new Random();
        
        Future<Object> pointGets = Executors.newSingleThreadExecutor().submit(() -> {

            long maxValueForPointGet = batchSize * (maxKeyIncrement + 1);
            int[] hits = {0};
            int[] misses = {0};
            long getStart = System.currentTimeMillis();
            while (stopGets.longValue() > System.currentTimeMillis()) {

                try {
                    if (running.isTrue()) {
                        Thread.sleep(100);
                        continue;
                    }

                    int longKey = rand.nextInt((int) maxValueForPointGet);
                    byte[] keyBytes = UIO.longBytes(longKey);
                    NextPointer pointer = indexs.getPointer(keyBytes);
                    if (!pointer.next((sortIndex, key, timestamp, tombstoned, version, pointer1) -> {
                        if (key != null) {
                            hits[0]++;
                        } else {
                            misses[0]++;
                        }
                        return true;
                    })) {
                        misses[0]++;
                    }
                    if ((hits[0] + misses[0]) % 10 == 0) {
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
                new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", false), maxLeaps, updatesBetweenLeaps);//,
            //new DiskBackedPointerIndexFiler(keysFile.getAbsolutePath(), "rw", false));

            PointerIndexUtils.append(index, 0, maxKeyIncrement, batchSize, null);
            indexs.append(index);
            count += batchSize;

            System.out.println("Insertions:" + format.format(count) + " ips:" + format.format(
                ((count / (double) (System.currentTimeMillis() - start))) * 1000) + " elapse:" + format.format(
                    (System.currentTimeMillis() - startMerge)) + " mergeDebut:" + indexs.mergeDebt());
        }

        running.setValue(false);
        merger.get();
        pointGets.get();

        System.out.println("Done. " + (System.currentTimeMillis() - start));

    }

}
