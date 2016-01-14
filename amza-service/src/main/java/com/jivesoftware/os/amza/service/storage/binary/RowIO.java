package com.jivesoftware.os.amza.service.storage.binary;

import com.jivesoftware.os.amza.api.wal.WALReader;
import com.jivesoftware.os.amza.api.wal.WALWriter;
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

    void initLeaps() throws Exception;

    boolean validate() throws Exception;

    void hackTruncation(int numBytes);

}
