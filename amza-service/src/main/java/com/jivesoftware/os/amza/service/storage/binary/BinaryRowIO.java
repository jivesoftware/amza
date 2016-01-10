package com.jivesoftware.os.amza.service.storage.binary;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.stream.Fps;
import com.jivesoftware.os.amza.api.stream.RowType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan.colt
 */
public class BinaryRowIO<K> implements RowIO<K> {

    private final K key;
    private final String name;
    private final BinaryRowReader rowReader;
    private final BinaryRowWriter rowWriter;
    private final int updatesBetweenLeaps;
    private final int maxLeaps;

    private final AtomicReference<LeapFrog> latestLeapFrog = new AtomicReference<>();
    private final AtomicLong updatesSinceLeap = new AtomicLong(0);

    public BinaryRowIO(K key,
        String name,
        BinaryRowReader rowReader,
        BinaryRowWriter rowWriter,
        int updatesBetweenLeaps,
        int maxLeaps) throws Exception {

        this.key = key;
        this.name = name;
        this.rowReader = rowReader;
        this.rowWriter = rowWriter;
        this.updatesBetweenLeaps = updatesBetweenLeaps;
        this.maxLeaps = maxLeaps;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public String getName() {
        return name;
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
            } else if (rowType.isPrimary()) {
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
    public void hackTruncation(int numBytes) {
        rowReader.hackTruncation(numBytes);
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
                    byte[] typeByteTxIdAndRow = readTypeByteTxIdAndRow(leaps.fpIndex[i]);
                    //TODO the leaps are basically fixed, so it would make sense to cache the fp -> leaps
                    next = Leaps.fromBytes(typeByteTxIdAndRow, 1 + 8, typeByteTxIdAndRow.length - (1 + 8));
                    break;
                }
            }
            leaps = next;
        }
        return closestFP;
    }

    @Override
    public boolean reverseScan(RowStream rowStream) throws Exception {
        return rowReader.reverseScan(rowStream);
    }

    @Override
    public byte[] readTypeByteTxIdAndRow(long fp) throws Exception {
        return rowReader.readTypeByteTxIdAndRow(fp);
    }

    @Override
    public boolean read(Fps fps, RowStream rowStream) throws Exception {
        return rowReader.read(fps, rowStream);
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
    public int write(long txId,
        RowType rowType,
        int estimatedNumberOfRows,
        int estimatedSizeInBytes,
        RawRows rows,
        IndexableKeys indexableKeys,
        TxKeyPointerFpStream stream) throws Exception {
        int count = rowWriter.write(txId, rowType, estimatedNumberOfRows, estimatedSizeInBytes, rows, indexableKeys, stream);
        if (updatesSinceLeap.addAndGet(count) >= updatesBetweenLeaps) {
            LeapFrog latest = latestLeapFrog.get();
            Leaps leaps = computeNextLeaps(txId, latest, maxLeaps);
            long leapFp = rowWriter.writeSystem(leaps.toBytes());
            latestLeapFrog.set(new LeapFrog(leapFp, leaps));
            updatesSinceLeap.set(0);
        }
        return count;
    }

    @Override
    public long getEndOfLastRow() throws Exception {
        return rowWriter.getEndOfLastRow();
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
        rowReader.close();
        rowWriter.close();
    }

    static private Leaps computeNextLeaps(long lastTransactionId, BinaryRowIO.LeapFrog latest, int maxLeaps) {
        long[] fpIndex;
        long[] transactionIds;
        if (latest == null) {
            fpIndex = new long[0];
            transactionIds = new long[0];
        } else if (latest.leaps.fpIndex.length < maxLeaps) {
            int numLeaps = latest.leaps.fpIndex.length + 1;
            fpIndex = new long[numLeaps];
            transactionIds = new long[numLeaps];
            System.arraycopy(latest.leaps.fpIndex, 0, fpIndex, 0, latest.leaps.fpIndex.length);
            System.arraycopy(latest.leaps.transactionIds, 0, transactionIds, 0, latest.leaps.transactionIds.length);
            fpIndex[numLeaps - 1] = latest.fp;
            transactionIds[numLeaps - 1] = latest.leaps.lastTransactionId;
        } else {
            fpIndex = new long[0];
            transactionIds = new long[maxLeaps];

            long[] idealFpIndex = new long[maxLeaps];
            // b^n = fp
            // b^32 = 123_456
            // ln b^32 = ln 123_456
            // 32 ln b = ln 123_456
            // ln b = ln 123_456 / 32
            // b = e^(ln 123_456 / 32)
            double base = Math.exp(Math.log(latest.fp) / maxLeaps);
            for (int i = 0; i < idealFpIndex.length; i++) {
                idealFpIndex[i] = latest.fp - (long) Math.pow(base, (maxLeaps - i - 1));
            }

            double smallestDistance = Double.MAX_VALUE;

            for (int i = 0; i < latest.leaps.fpIndex.length; i++) {
                long[] testFpIndex = new long[maxLeaps];

                System.arraycopy(latest.leaps.fpIndex, 0, testFpIndex, 0, i);
                System.arraycopy(latest.leaps.fpIndex, i + 1, testFpIndex, i, maxLeaps - 1 - i);
                testFpIndex[maxLeaps - 1] = latest.fp;

                double distance = euclidean(testFpIndex, idealFpIndex);
                if (distance < smallestDistance) {
                    fpIndex = testFpIndex;
                    System.arraycopy(latest.leaps.transactionIds, 0, transactionIds, 0, i);
                    System.arraycopy(latest.leaps.transactionIds, i + 1, transactionIds, i, maxLeaps - 1 - i);
                    transactionIds[maxLeaps - 1] = latest.leaps.lastTransactionId;
                    smallestDistance = distance;
                }
            }

            //System.out.println("@" + latest.fp + " base:  " + base);
            //System.out.println("@" + latest.fp + " ideal: " + Arrays.toString(idealFpIndex));
            //System.out.println("@" + latest.fp + " next:  " + Arrays.toString(fpIndex));
        }

        return new Leaps(lastTransactionId, fpIndex, transactionIds);
    }

    static private double euclidean(long[] a, long[] b) {
        double v = 0;
        for (int i = 0; i < a.length; i++) {
            double d = a[i] - b[i];
            v += d * d;
        }
        return Math.sqrt(v);
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

        private static Leaps fromBytes(byte[] bytes, int offset, int length) {
            ByteBuffer buf = ByteBuffer.wrap(bytes, offset, length);
            return fromByteBuffer(buf);
        }

        private static Leaps fromByteBuffer(ByteBuffer buf) {
            buf.getLong(); // just read over 8 bytes
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
