package com.jivesoftware.os.amza.storage.binary;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowReader;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.RowsIndex;
import com.jivesoftware.os.amza.shared.RowsTx;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.filer.Filer;
import com.jivesoftware.os.amza.storage.filer.IFiler;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 *
 * @author jonathan.colt
 */
public class BinaryRowsTx implements RowsTx<byte[]> {

    private static final int numPermits = 1024;
    private final Semaphore compactionLock = new Semaphore(numPermits, true);
    private final File tableFile;
    private final RowMarshaller<byte[]> rowMarshaller;
    private IFiler filer;
    private BinaryRowReader rowReader;
    private BinaryRowWriter rowWriter;

    public BinaryRowsTx(File tableFile, RowMarshaller<byte[]> rowMarshaller) throws IOException {
        this.tableFile = tableFile;
        this.rowMarshaller = rowMarshaller;
        filer = new Filer(tableFile.getAbsolutePath(), "rw");
        rowReader = new BinaryRowReader(filer);
        rowWriter = new BinaryRowWriter(filer);
    }

    @Override
    public <R> R write(RowsWrite<byte[], R> write) throws Exception {
        compactionLock.acquire();
        try {
            return write.write(rowWriter);
        } finally {
            compactionLock.release();
        }
    }

    @Override
    public <R> R read(RowsRead<byte[], R> read) throws Exception {
        compactionLock.acquire();
        try {
            return read.read(rowReader);
        } finally {
            compactionLock.release();
        }
    }

    @Override
    public void compact(final RowsIndex rowsIndex) throws Exception {
        compactionLock.acquire(numPermits);
        try {
            File dir = Files.createTempDir();
            String name = tableFile.getName();
            File compactTo = new File(dir, name);
            try (Filer compactionFiler = new Filer(compactTo.getAbsolutePath(), "rw")) {
                final BinaryRowWriter compactionWriter = new BinaryRowWriter(compactionFiler);

                rowReader.scan(false, new RowReader.Stream<byte[]>() {
                    @Override
                    public boolean row(final byte[] rowPointer, final byte[] row) throws Exception {
                        rowMarshaller.fromRow(row, new RowScan() {

                            @Override
                            public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws Exception {

                                RowIndexValue got = rowsIndex.get(key);
                                if (got == null || value.getTimestamp() >= got.getTimestamp()) {
                                    List<byte[]> rows = new ArrayList<>(); //  TODO batching.
                                    rows.add(row);
                                    List<byte[]> newPointers = compactionWriter.write(rows, true);
                                    // TODO fix rowPointers in rowIndex
                                    //System.out.println("kept key:" + new String(key.getKey())
                                    //+ " value:" + new String(value.getValue()) + " " + " ts:" + value
                                    //    .getTimestamp() + " " + tableFile);

                                } else {
                                    //System.out.println("removed key:" + new String(key.getKey()) + " ts:" + value.getTimestamp() + " " + tableFile);
                                }
                                return true;
                            }
                        });
                        return true;
                    }
                });
            }
            filer.close(); // HACKY as HELL

            File backup = new File(tableFile.getParentFile(), "bkp-" + tableFile.getName());
            backup.delete();

            long sizeBeforeCompaction = tableFile.length();
            long sizeAfterCompaction = compactTo.length();
            Files.move(tableFile, backup);
            Files.move(compactTo, tableFile);

            // Reopen the world
            filer = new Filer(tableFile.getAbsolutePath(), "rw"); // HACKY as HELL
            rowReader = new BinaryRowReader(filer); // HACKY as HELL
            rowWriter = new BinaryRowWriter(filer); // HACKY as HELL

            System.out.println("Compacted table " + tableFile.getAbsolutePath()
                + " was:" + sizeBeforeCompaction + "bytes "
                + " isNow:" + sizeAfterCompaction + "bytes.");
        } finally {
            compactionLock.release(numPermits);
        }
    }

}
