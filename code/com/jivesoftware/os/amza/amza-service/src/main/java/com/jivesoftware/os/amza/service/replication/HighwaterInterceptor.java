package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALValue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class HighwaterInterceptor implements WALScanable {

    private final String context;
    private final long initialHighwaterMark;
    private final AtomicLong highaterMark;
    private final WALScanable scanable;

    public HighwaterInterceptor(String context, Long initialHighwaterMark, WALScanable scanable) {
        this.context = context;
        this.initialHighwaterMark = initialHighwaterMark == null ? -1 : initialHighwaterMark;
        this.highaterMark = new AtomicLong(this.initialHighwaterMark);
        this.scanable = scanable;
    }

    public long getHighwater() {
        return highaterMark.get();
    }

    @Override
    public void rowScan(final WALScan walScan) throws Exception {
        scanable.rowScan(new WALScanHighwaterInterceptor(walScan));
    }

    @Override
    public void rangeScan(WALKey from, WALKey to, final WALScan walScan) throws Exception {
        scanable.rangeScan(from, to, new WALScanHighwaterInterceptor(walScan));
    }

    class WALScanHighwaterInterceptor implements WALScan {

        private final WALScan walScan;

        public WALScanHighwaterInterceptor(WALScan walScan) {
            this.walScan = walScan;
        }

        @Override
        public boolean row(long transactionId, WALKey key, WALValue value) throws Exception {
            if (transactionId <= initialHighwaterMark) {
                return true;
            }
            if (walScan.row(transactionId, key, value)) {
                long got = highaterMark.get();
                while (got < transactionId) {
                    highaterMark.compareAndSet(got, transactionId);
                    got = highaterMark.get();
                }
                return true;
            } else {
                return false;
            }
        }
    }
}
