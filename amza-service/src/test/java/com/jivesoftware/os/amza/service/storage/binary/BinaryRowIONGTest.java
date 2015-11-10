package com.jivesoftware.os.amza.service.storage.binary;

import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.service.storage.filer.DiskBackedWALFiler;
import com.jivesoftware.os.amza.service.storage.filer.MemoryBackedWALFiler;
import com.jivesoftware.os.amza.shared.filer.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.amza.shared.filer.HeapByteBufferFactory;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.jivesoftware.os.amza.service.storage.binary.BinaryRowIO.UPDATES_BETWEEN_LEAPS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class BinaryRowIONGTest {

    @Test
    public void testDiskWrite() throws Exception {
        File file = File.createTempFile("BinaryRowIO", "dat");
        DiskBackedWALFiler filer = new DiskBackedWALFiler(file.getAbsolutePath(), "rw", false);
        IoStats ioStats = new IoStats();
        BinaryRowIO<File> binaryRowIO = new BinaryRowIO<>(file,
            new BinaryRowReader(filer, ioStats, 10),
            new BinaryRowWriter(filer, ioStats));
        write(10_000, () -> binaryRowIO);
    }

    @Test
    public void testMemoryWrite() throws Exception {
        MemoryBackedWALFiler filer = new MemoryBackedWALFiler(new AutoGrowingByteBufferBackedFiler(1_024, 1_024 * 1_024,
            new HeapByteBufferFactory()));
        IoStats ioStats = new IoStats();
        BinaryRowIO binaryRowIO = new BinaryRowIO<>(filer,
            new BinaryRowReader(filer, ioStats, 10),
            new BinaryRowWriter(filer, ioStats));
        write(500, () -> binaryRowIO);
    }

    private void write(int numRows, Callable<BinaryRowIO> reopen) throws Exception {
        BinaryRowIO rowIO = reopen.call();

        List<Long> rowTxIds = Lists.newArrayList();
        List<byte[]> rows = Lists.newArrayList();
        long nextTxId = 1;
        Random r = new Random();
        for (long i = 0; i < numRows; i++) {
            if (r.nextInt(10) == 0) {
                nextTxId++;
            }
            rowTxIds.add(nextTxId);
            rows.add(UIO.longBytes(i));
            byte[] row = UIO.longBytes(i);
            rowIO.write(nextTxId,
                RowType.primary,
                1,
                row.length,
                stream -> stream.stream(row),
                stream -> true,
                (txId, prefix, key, valueTimestamp, valueTombstoned, valueVersion, fp) -> true);
        }

        rowIO = reopen.call();

        rowIO.scan(0, false, new RowStream() {

            private long lastTxId = -1;
            private long expectedValue = 0;
            private boolean lastWasLeap = false;

            @Override
            public boolean row(long rowFP, long rowTxId, RowType rowType, byte[] row) throws Exception {
                if (rowType == RowType.primary) {
                    if (lastWasLeap) {
                        assertTrue(rowTxId > lastTxId);
                        lastWasLeap = false;
                    }

                    assertTrue(rowTxId >= lastTxId);
                    lastTxId = rowTxId;

                    assertEquals(UIO.bytesLong(row), expectedValue);
                    expectedValue++;
                } else if (rowType == RowType.system) {
                    lastWasLeap = true;
                }
                return false;
            }
        });
    }

    @Test
    public void testDiskLeap() throws Exception {
        File file = File.createTempFile("BinaryRowIO", "dat");
        DiskBackedWALFiler filer = new DiskBackedWALFiler(file.getAbsolutePath(), "rw", false);
        IoStats ioStats = new IoStats();
        leap(() -> new BinaryRowIO<>(file, new BinaryRowReader(filer, ioStats, 10), new BinaryRowWriter(filer, ioStats)));
    }

    @Test
    public void testMemoryLeap() throws Exception {
        MemoryBackedWALFiler filer = new MemoryBackedWALFiler(new AutoGrowingByteBufferBackedFiler(1_024, 1_024 * 1_024,
            new HeapByteBufferFactory()));
        IoStats ioStats = new IoStats();
        BinaryRowIO binaryRowIO = new BinaryRowIO<>(filer, new BinaryRowReader(filer, ioStats, 10), new BinaryRowWriter(filer, ioStats));
        leap(() -> binaryRowIO);
    }

    private void leap(Callable<BinaryRowIO> reopen) throws Exception {
        BinaryRowIO rowIO = reopen.call();
        int numRows = 10_000;

        for (long i = 0; i < numRows; i++) {
            byte[] row = UIO.longBytes(i);
            rowIO.write(i,
                RowType.primary,
                1,
                row.length,
                stream -> stream.stream(row),
                stream -> true,
                (txId, prefix, key, valueTimestamp, valueTombstoned, valueVersion, fp) -> true);
            /*if (i % 10_000 == 0) {
             System.out.println("Wrote " + i);
             }*/
        }

        rowIO = reopen.call();
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
                public boolean row(long rowFP, long rowTxId, RowType rowType, byte[] row) throws Exception {
                    if (rowType == RowType.system) {
                        return true;
                    } else if (rowType == RowType.primary) {
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
                    return true;
                }
            });
        }

        /*for (int i = 0; i < histo.length; i++) {
         System.out.println("[" + i + "] " + histo[i]);
         }*/
    }
}
