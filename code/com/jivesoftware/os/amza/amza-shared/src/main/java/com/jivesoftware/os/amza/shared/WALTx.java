package com.jivesoftware.os.amza.shared;

import com.google.common.base.Optional;

/**
 *
 * @author jonathan.colt
 */
public interface WALTx {

    public static interface WALWrite<R> {

        R write(WALWriter writer) throws Exception;
    }

    public static interface WALRead<R> {

        R read(WALReader reader) throws Exception;
    }

    <R> R write(WALWrite<R> write) throws Exception;

    <R> R read(WALRead<R> read) throws Exception;

    WALIndex load(RegionName regionName) throws Exception;

    Optional<Compacted> compact(RegionName regionName,
        long removeTombstonedOlderThanTimestampId,
        long ttlTimestampId,
        WALIndex rowIndex) throws Exception;

    public interface Compacted {

        WALIndex getCompactedWALIndex() throws Exception;
    }
}
