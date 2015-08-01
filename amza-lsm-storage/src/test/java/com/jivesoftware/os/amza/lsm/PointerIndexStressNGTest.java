package com.jivesoftware.os.amza.lsm;

import java.io.File;
import java.text.NumberFormat;
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
        PointerIndexs indexs = new PointerIndexs();
        int count = 0;
        int batchSize = 10_000;
        for (int b = 0; b < 10; b++) {

            MemoryPointerIndex walIndex = new MemoryPointerIndex();
            PointerIndexUtils.append(walIndex, b * batchSize, 2, batchSize, null);
            indexs.append(walIndex);
            count += batchSize;

            int _b = b;
            long startMerge = System.currentTimeMillis();
            if (indexs.merge(() -> {
                File indexFiler = File.createTempFile("d-index-merged-" + _b, ".tmp");
                File keysFile = File.createTempFile("d-keys-merged-" + _b, ".tmp");

                return new PointerIndex(new DiskBackedPointerIndexFiler(indexFiler.getAbsolutePath(), "rw", false),
                    new DiskBackedPointerIndexFiler(keysFile.getAbsolutePath(), "rw", false));
            })) {
                System.out.println("Merge elapse:" + format.format((System.currentTimeMillis() - startMerge)));
            }
            System.out.println("Insertions:" + format.format(count) + " ips:" + format.format(
                (((double) count / (double) (System.currentTimeMillis() - start))) * 1000) + " elapse:" + format.format(
                    (System.currentTimeMillis() - startMerge)));
        }

    }

}
