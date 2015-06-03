package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan.colt
 */
public class BinaryRowIO<K> implements RowIO<K> {

    private static final int MAX_LEAPS = 64; //TODO config?
    public static final int UPDATES_BETWEEN_LEAPS = 4_096; //TODO config?

    private final ManageRowIO<K> manageRowIO;
    private final K filerKey;
    private final BinaryRowReader rowReader;
    private final BinaryRowWriter rowWriter;

    private final AtomicReference<LeapFrog> latestLeapFrog = new AtomicReference<>();
    private final AtomicLong updatesSinceLeap = new AtomicLong(0);

    public BinaryRowIO(ManageRowIO<K> manageRowIO, K filerKey,
        BinaryRowReader rowReader,
        BinaryRowWriter rowWriter) throws Exception {

        this.manageRowIO = manageRowIO;
        this.filerKey = filerKey;
        this.rowReader = rowReader;
        this.rowWriter = rowWriter;
    }

    @Override
    public void initLeaps() throws Exception {
        final MutableLong updates = new MutableLong(0);

        reverseScan((long rowFP, long rowTxId, RowType rowType, byte[] row) -> {
            if (rowType == RowType.system) {
                ByteBuffer buf = ByteBuffer.wrap(row);
                byte[] keyBytes = new byte[8];
                buf.get(keyBytes);
                long key = UIO.bytesLong(keyBytes);
                if (key == RowType.LEAP_KEY) {
                    buf.rewind();
                    latestLeapFrog.set(new LeapFrog(rowFP, Leaps.fromByteBuffer(buf)));
                    return false;
                }
            } else if (rowType == RowType.primary) {
                updates.increment();
            }
            return true;
        });

        updatesSinceLeap.addAndGet(updates.longValue());
    }

    @Override
    public boolean validate() throws Exception {
        return rowReader.validate();
    }

    @Override
    public boolean scan(long offsetFp, boolean allowRepairs, RowStream rowStream) throws Exception {
        return rowReader.scan(offsetFp, allowRepairs, rowStream);
    }

    @Override
    public long getInclusiveStartOfRow(long transactionId) throws Exception {
        LeapFrog leapFrog = latestLeapFrog.get();
        Leaps leaps = (leapFrog != null) ? leapFrog.leaps : null;

        long closestFP = 0;
        while (leaps != null) {
            Leaps next = null;
            for (int i = 0; i < leaps.transactionIds.length; i++) {
                if (leaps.transactionIds[i] < transactionId) {
                    closestFP = Math.max(closestFP, leaps.fpIndex[i]);
                } else {
                    //TODO the leaps are basically fixed, so it would make sense to cache the fp -> leaps
                    next = Leaps.fromBytes(read(leaps.fpIndex[i]));
                    break;
                }
            }
            leaps = next;
        }
        return closestFP;
    }

    @Override
    public void reverseScan(RowStream rowStream) throws Exception {
        rowReader.reverseScan(rowStream);
    }

    @Override
    public byte[] read(long pointer) throws Exception {
        return rowReader.read(pointer);
    }

    @Override
    public long writeHighwater(byte[] row) throws Exception {
        return rowWriter.writeHighwater(row);
    }

    @Override
    public long writeSystem(byte[] row) throws Exception {
        return rowWriter.writeSystem(row);
    }

    @Override
    public long[] writePrimary(List<Long> txId, List<byte[]> rows) throws Exception {
        return write(txId, Collections.nCopies(txId.size(), RowType.primary), rows);
    }

    @Override
    public long[] write(List<Long> rowTxIds, List<RowType> rowTypes, List<byte[]> rows) throws Exception {
        long lastTxId = Long.MIN_VALUE;
        long[] fps = new long[rows.size()];
        int lastWriteIndex = 0;
        for (int i = 0; i < rowTxIds.size(); i++) {
            long currentTxId = rowTxIds.get(i);
            if (lastTxId != Long.MIN_VALUE && lastTxId != currentTxId) {
                writeToWAL(rowTxIds, rowTypes, rows, fps, lastWriteIndex, i, currentTxId);
                lastWriteIndex = i;
            }
            lastTxId = currentTxId;
        }
        writeToWAL(rowTxIds, rowTypes, rows, fps, lastWriteIndex, rowTxIds.size(), lastTxId);
        return fps;
    }

    private void writeToWAL(List<Long> rowTxIds,
        List<RowType> rowTypes,
        List<byte[]> rows,
        long[] fps,
        int fromIndex,
        int toIndex,
        long currentTxId) throws Exception {

        if (toIndex == fromIndex) {
            return;
        }

        long[] currentFps = rowWriter.write(rowTxIds.subList(fromIndex, toIndex),
            rowTypes.subList(fromIndex, toIndex),
            rows.subList(fromIndex, toIndex));
        System.arraycopy(currentFps, 0, fps, fromIndex, currentFps.length);

        if (updatesSinceLeap.addAndGet(currentFps.length) >= UPDATES_BETWEEN_LEAPS) {
            LeapFrog latest = latestLeapFrog.get();

            Leaps leaps = computeNextLeaps(currentTxId, latest);
            long leapFp = rowWriter.writeSystem(leaps.toBytes());

            latestLeapFrog.set(new LeapFrog(leapFp, leaps));
            updatesSinceLeap.set(0);
        }
    }

    @Override
    public long getEndOfLastRow() throws Exception {
        return rowWriter.getEndOfLastRow();
    }

    @Override
    public void move(K destination) throws Exception {
        manageRowIO.move(filerKey, destination);
    }

    @Override
    public long sizeInBytes() throws Exception {
        return rowWriter.length();
    }

    @Override
    public void flush(boolean fsync) throws Exception {
        rowWriter.flush(fsync);
    }

    @Override
    public void close() throws IOException {
        rowWriter.close();
    }

    @Override
    public void delete() throws Exception {
        manageRowIO.delete(filerKey);
    }

    static private Leaps computeNextLeaps(long lastTransactionId, BinaryRowIO.LeapFrog latest) {
        long[] fpIndex;
        long[] transactionIds;
        if (latest == null) {
            fpIndex = new long[0];
            transactionIds = new long[0];
        } else if (latest.leaps.fpIndex.length < MAX_LEAPS) {
            int numLeaps = latest.leaps.fpIndex.length + 1;
            fpIndex = new long[numLeaps];
            transactionIds = new long[numLeaps];
            System.arraycopy(latest.leaps.fpIndex, 0, fpIndex, 0, latest.leaps.fpIndex.length);
            System.arraycopy(latest.leaps.transactionIds, 0, transactionIds, 0, latest.leaps.transactionIds.length);
            fpIndex[numLeaps - 1] = latest.fp;
            transactionIds[numLeaps - 1] = latest.leaps.lastTransactionId;
        } else {
            fpIndex = null;
            transactionIds = new long[MAX_LEAPS];

            long[] idealFpIndex = new long[MAX_LEAPS];
            // b^n = fp
            // b^32 = 123_456
            // ln b^32 = ln 123_456
            // 32 ln b = ln 123_456
            // ln b = ln 123_456 / 32
            // b = e^(ln 123_456 / 32)
            double base = Math.exp(Math.log(latest.fp) / MAX_LEAPS);
            for (int i = 0; i < idealFpIndex.length; i++) {
                idealFpIndex[i] = latest.fp - (long) Math.pow(base, (MAX_LEAPS - i - 1));
            }

            double smallestDistance = Double.MAX_VALUE;

            for (int i = 0; i < latest.leaps.fpIndex.length; i++) {
                long[] testFpIndex = new long[MAX_LEAPS];

                System.arraycopy(latest.leaps.fpIndex, 0, testFpIndex, 0, i);
                System.arraycopy(latest.leaps.fpIndex, i + 1, testFpIndex, i, MAX_LEAPS - 1 - i);
                testFpIndex[MAX_LEAPS - 1] = latest.fp;

                double distance = euclidean(testFpIndex, idealFpIndex);
                if (distance < smallestDistance) {
                    fpIndex = testFpIndex;
                    System.arraycopy(latest.leaps.transactionIds, 0, transactionIds, 0, i);
                    System.arraycopy(latest.leaps.transactionIds, i + 1, transactionIds, i, MAX_LEAPS - 1 - i);
                    transactionIds[MAX_LEAPS - 1] = latest.leaps.lastTransactionId;
                    smallestDistance = distance;
                }
            }

            //System.out.println("@" + latest.fp + " base:  " + base);
            //System.out.println("@" + latest.fp + " ideal: " + Arrays.toString(idealFpIndex));
            //System.out.println("@" + latest.fp + " next:  " + Arrays.toString(fpIndex));
        }

        return new Leaps(lastTransactionId, fpIndex, transactionIds);
    }

    static private double dumb(long[] a, long[] b) {
        double d = 0;
        for (int i = 0; i < a.length; i++) {
            d += Math.abs(a[i] - b[i]);
        }
        return d;
    }

    static private double euclidean(long[] a, long[] b) {
        double v = 0;
        for (int i = 0; i < a.length; i++) {
            long d = a[i] - b[i];
            v += d * d;
        }
        return Math.sqrt(v);
    }

    static private double acos(long[] a1, long[] b1) {
        double[] a = new double[a1.length];
        double[] b = new double[b1.length];
        for (int i = 0; i < a1.length; i++) {
            a[i] = (double) a1[i] / (double) a1[a1.length - 1];
            b[i] = (double) b1[i] / (double) b1[b1.length - 1];
        }
        double la = length(a);
        double lb = length(b);
        return Math.acos(dotProduct(a, b) / (la * lb));
    }

    static private double length(double... vs) {
        double v = 0;
        for (double a : vs) {
            v += a;
        }
        return Math.sqrt(v);
    }

    static public double dotProduct(double[] a, double[] b) {
        double dp = 0;
        for (int i = 0; i < a.length; i++) {
            dp += a[i] * b[i];
        }
        return dp;
    }

    private static class LeapFrog {

        private final long fp;
        private final Leaps leaps;

        public LeapFrog(long fp, Leaps leaps) {
            this.fp = fp;
            this.leaps = leaps;
        }
    }

    private static class Leaps {

        private final long lastTransactionId;
        private final long[] fpIndex;
        private final long[] transactionIds;

        public Leaps(long lastTransactionId, long[] fpIndex, long[] transactionIds) {
            this.lastTransactionId = lastTransactionId;
            this.fpIndex = fpIndex;
            this.transactionIds = transactionIds;
        }

        private byte[] toBytes() {
            ByteBuffer buf = ByteBuffer.wrap(new byte[8 + 8 + 4 + fpIndex.length * 16]);
            buf.put(UIO.longBytes(RowType.LEAP_KEY));
            buf.putLong(lastTransactionId);
            buf.putInt(fpIndex.length);
            for (int i = 0; i < fpIndex.length; i++) {
                buf.putLong(fpIndex[i]);
                buf.putLong(transactionIds[i]);
            }
            return buf.array();
        }

        private static Leaps fromBytes(byte[] bytes) {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            return fromByteBuffer(buf);
        }

        private static Leaps fromByteBuffer(ByteBuffer buf) {
            long key = buf.getLong(); // just read over 8 bytes
            long lastTransactionId = buf.getLong();
            int length = buf.getInt();
            long[] fpIndex = new long[length];
            long[] transactionIds = new long[length];
            for (int i = 0; i < length; i++) {
                fpIndex[i] = buf.getLong();
                transactionIds[i] = buf.getLong();
            }
            return new Leaps(lastTransactionId, fpIndex, transactionIds);
        }
    }

}
