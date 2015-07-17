package com.jivesoftware.os.amza.service.storage.binary;

import com.jivesoftware.os.amza.shared.wal.WALReader;
import com.jivesoftware.os.amza.shared.wal.WALWriter;

/**
 * @author jonathan.colt
 */
public interface RowIO<K> extends WALReader, WALWriter {

    long getInclusiveStartOfRow(long transactionId) throws Exception;

    void move(K destination) throws Exception;

    long sizeInBytes() throws Exception;

    void flush(boolean fsync) throws Exception;

    void close() throws Exception;

    void delete() throws Exception;

    void initLeaps() throws Exception;

    boolean validate() throws Exception;
}
