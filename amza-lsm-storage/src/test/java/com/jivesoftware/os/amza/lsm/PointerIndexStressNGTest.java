package com.jivesoftware.os.amza.lsm;

import java.io.File;
import java.text.NumberFormat;
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
        long start = System.currentTimeMillis();
        MergeablePointerIndexs indexs = new MergeablePointerIndexs();
        int maxDepthBeforeMerging = 4;
        int count = 0;
        int batchSize = 10_000;

        MutableLong merge = new MutableLong();
        MutableBoolean running = new MutableBoolean(true);
        Future<Object> merger = Executors.newSingleThreadExecutor().submit(() -> {
            while (running.isTrue() || indexs.mergeDebut() > maxDepthBeforeMerging) {
                try {
                    long startMerge = System.currentTimeMillis();
                    if (indexs.merge(maxDepthBeforeMerging, () -> {

                        merge.increment();
                        File mergeIndexFiler = File.createTempFile("d-index-merged-" + merge.intValue(), ".tmp");
                        File mergeKeysFile = File.createTempFile("d-keys-merged-" + merge.intValue(), ".tmp");

                        return new DiskBackedPointerIndex(
                            new DiskBackedPointerIndexFiler(mergeIndexFiler.getAbsolutePath(), "rw", false),
                            new DiskBackedPointerIndexFiler(mergeKeysFile.getAbsolutePath(), "rw", false)
                        );
                    })) {
                        System.out.println("Merge (" + merge.intValue() + ") elapse:" + format.format((System.currentTimeMillis() - startMerge)));
                    } else {
                        System.out.println("Nothing to merge. Sleeping.");
                        Thread.sleep(1000);
                    }
                } catch (Exception x) {
                    x.printStackTrace();
                    Thread.sleep(10_000);
                }
            }
            return null;

        });

        for (int b = 0; b < 1000; b++) {

            File indexFiler = File.createTempFile("s-index-merged-" + b, ".tmp");
            File keysFile = File.createTempFile("s-keys-merged-" + b, ".tmp");

            //MemoryPointerIndex index = new MemoryPointerIndex();
            long startMerge = System.currentTimeMillis();

            DiskBackedPointerIndex index = new DiskBackedPointerIndex(
                new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", false),
                new DiskBackedPointerIndexFiler(keysFile.getAbsolutePath(), "rw", false));

            PointerIndexUtils.append(index, b * batchSize, 2, batchSize, null);
            indexs.append(index);
            count += batchSize;

            System.out.println("Insertions:" + format.format(count) + " ips:" + format.format(
                (((double) count / (double) (System.currentTimeMillis() - start))) * 1000) + " elapse:" + format.format(
                    (System.currentTimeMillis() - startMerge)) + " mergeDebut:" + indexs.mergeDebut());
        }

        running.setValue(false);
        merger.get();

        System.out.println("Done. " + (System.currentTimeMillis() - start));

    }

}
