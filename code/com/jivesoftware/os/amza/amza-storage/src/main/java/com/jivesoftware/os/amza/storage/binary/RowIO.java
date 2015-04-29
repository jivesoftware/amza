package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.WALReader;
import com.jivesoftware.os.amza.shared.WALWriter;
import java.io.File;

/**
 *
 * @author jonathan.colt
 */
public interface RowIO extends WALReader, WALWriter {

    long getInclusiveStartOfRow(long transactionId) throws Exception;

    void move(File destinationDir) throws Exception;

    long sizeInBytes() throws Exception;

    void flush(boolean fsync) throws Exception;

    void close() throws Exception;

    void delete() throws Exception;

    void initLeaps() throws Exception;

    boolean validate() throws Exception;
}
