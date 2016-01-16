package com.jivesoftware.os.amza.api.wal;

import com.jivesoftware.os.amza.api.stream.RowType;
import java.io.File;

/**
 * @author jonathan.colt
 */
public interface RowIO extends WALReader, WALWriter {

    File getKey();

    String getName();

    long getInclusiveStartOfRow(long transactionId) throws Exception;

    long sizeInBytes() throws Exception;

    void flush(boolean fsync) throws Exception;

    void close() throws Exception;

    void initLeaps(long fpOfLastLeap, long updates) throws Exception;

    long getUpdatesSinceLeap();

    long getFpOfLastLeap();

    void validate(boolean truncateToEndOfMergeMarker,
        ValidationStream backward,
        ValidationStream forward,
        ValidationNotifier validationNotifier) throws Exception;

    void hackTruncation(int numBytes);

    interface ValidationStream {

        /**

        @param rowFP
        @param rowTxId
        @param rowType
        @param row
        @return truncateAfterRowAtFp. -1 means continue
        @throws Exception
         */
        long row(long rowFP, long rowTxId, RowType rowType, byte[] row) throws Exception;

    }

    interface ValidationNotifier {

        void corrupt(long corruptAtFP, boolean reverse);
    }
}
