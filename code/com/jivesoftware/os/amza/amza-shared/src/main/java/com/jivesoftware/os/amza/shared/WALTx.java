package com.jivesoftware.os.amza.shared;

import com.google.common.base.Optional;

/**
 *
 * @author jonathan.colt
 */
public interface WALTx {

    <R> R write(WALWrite<R> write) throws Exception;

    <R> R read(WALRead<R> read) throws Exception;

    WALIndex load(RegionName regionName) throws Exception;

    boolean delete(boolean ifEmpty) throws Exception;

    Optional<Compacted> compact(RegionName regionName,
        long removeTombstonedOlderThanTimestampId,
        long ttlTimestampId,
        WALIndex rowIndex) throws Exception;

    interface WALWrite<R> {

        R write(WALWriter writer) throws Exception;
    }

    interface WALRead<R> {

        R read(WALReader reader) throws Exception;
    }

    interface Compacted {

        CommittedCompacted commit() throws Exception;
    }

    class CommittedCompacted {

        public final long sizeInBytes;
        public final WALIndex index;

        public CommittedCompacted(long sizeInBytes, WALIndex index) {
            this.sizeInBytes = sizeInBytes;
            this.index = index;
        }
    }
}
