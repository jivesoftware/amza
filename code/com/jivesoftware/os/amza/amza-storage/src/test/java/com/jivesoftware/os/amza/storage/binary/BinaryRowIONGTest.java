package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.WALWriter;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.storage.filer.WALFiler;
import java.io.File;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.jivesoftware.os.amza.storage.binary.BinaryRowIO.UPDATES_BETWEEN_LEAPS;

/**
 *
 */
public class BinaryRowIONGTest {

    @Test
    public void testLeap() throws Exception {
        File file = File.createTempFile("BinaryRowIO", "dat");
        WALFiler filer = new WALFiler(file.getAbsolutePath(), "rw");
        IoStats ioStats = new IoStats();
        BinaryRowIO rowIO = new BinaryRowIO(file, filer, new BinaryRowReader(filer, ioStats, 10), new BinaryRowWriter(filer, ioStats));
        int numRows = 10_000;

        for (long i = 0; i < numRows; i++) {
            rowIO.write(Collections.singletonList(i),
                Collections.singletonList(WALWriter.VERSION_1),
                Collections.singletonList(UIO.longBytes(i)));
            /*if (i % 10_000 == 0) {
                System.out.println("Wrote " + i);
            }*/
        }

        rowIO = new BinaryRowIO(file, filer, new BinaryRowReader(filer, ioStats, 10), new BinaryRowWriter(filer, ioStats));

        /*long[][] histos = new long[32][];
        for (int i = 0; i < histos.length; i++) {
            histos[i] = new long[16];
        }

        long start = System.currentTimeMillis();
        for (long i = 0; i < numRows; i++) {
            long[] result = rowIO.getInclusive(i);
            long[] histo = histos[(int) Math.log(i + 1)];
            histo[Math.min((int) result[1], histo.length - 1)]++;
            if (i % 10_000 == 0) {
                System.out.println("Got " + i + " in " + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            }
        }
        for (int i = 0; i < histos.length; i++) {
            System.out.println("----------------------------- " + i + " -----------------------------");
            for (int j = 0; j < histos[i].length; j++) {
                System.out.println("[" + j + "] " + histos[i][j]);
            }
        }*/

        final int[] histo = new int[UPDATES_BETWEEN_LEAPS];
        for (long i = 0; i < numRows; i += (UPDATES_BETWEEN_LEAPS / 10)) {
            final long txId = i;
            long nextStartOfRow = rowIO.getInclusiveStartOfRow(txId);
            rowIO.scan(nextStartOfRow, false, new RowStream() {

                private int count = 0;

                @Override
                public boolean row(long rowFP, long rowTxId, byte rowType, byte[] row) throws Exception {
                    if (rowType < 0) {
                        return true;
                    }
                    if (rowTxId == txId) {
                        histo[count]++;
                        return false;
                    } else if (rowTxId > txId) {
                        Assert.fail("Missed the desired txId: " + rowTxId + " > " + txId);
                        return false;
                    } else {
                        if (count == UPDATES_BETWEEN_LEAPS) {
                            Assert.fail("Gave up without seeing the desired txId: " + txId);
                        }
                        count++;
                        return count < UPDATES_BETWEEN_LEAPS;
                    }
                }
            });
        }

        /*for (int i = 0; i < histo.length; i++) {
            System.out.println("[" + i + "] " + histo[i]);
        }*/
    }
}