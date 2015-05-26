package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowType;
import com.jivesoftware.os.amza.shared.WALStorage;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class HighwaterInterceptor {

    private final long initialHighwaterMark;
    private final AtomicLong highaterMark;
    private final WALStorage wal;

    public HighwaterInterceptor(Long initialHighwaterMark, WALStorage wal) {
        this.initialHighwaterMark = initialHighwaterMark == null ? -1 : initialHighwaterMark;
        this.highaterMark = new AtomicLong(this.initialHighwaterMark);
        this.wal = wal;
    }

    public long getHighwater() {
        return highaterMark.get();
    }

    public void rowScan(final RowStream rowStream) throws Exception {
        wal.takeRowUpdatesSince(initialHighwaterMark, new WALScanHighwaterInterceptor(rowStream));
    }

    class WALScanHighwaterInterceptor implements RowStream {

        private final RowStream rowStream;

        public WALScanHighwaterInterceptor(RowStream rowStream) {
            this.rowStream = rowStream;
        }

        @Override
        public boolean row(long rowFP, long rowTxId, RowType rowType, byte[] row) throws Exception {

            if (rowType == RowType.primary) {
                if (rowTxId <= initialHighwaterMark) {
                    return true;
                }

                if (rowStream.row(rowFP, rowTxId, rowType, row)) {
                    long got = highaterMark.get();
                    while (got < rowTxId) {
                        highaterMark.compareAndSet(got, rowTxId);
                        got = highaterMark.get();
                    }
                    return true;
                } else {
                    return false;
                }
            }
            return true;
        }

    }
}
