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

    Optional<Compacted> compact(long removeTombstonedOlderThanTimestampId,
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

        public final WALIndex index;
        public final long sizeBeforeCompaction;
        public final long sizeAfterCompaction;
        public final long keyCount;
        public final long removeCount;
        public final long tombstoneCount;
        public final long ttlCount;
        public final long duration;
        public final long catchupKeys;
        public final long catchupRemoves;
        public final long catchupTombstones;
        public final long catchupTTL;
        public final long catchupDuration;

        public CommittedCompacted(WALIndex index, long sizeBeforeCompaction, long sizeAfterCompaction, long keyCount, long removeCount,
            long tombstoneCount, long ttlCount, long duration, long catchupKeys, long catchupRemoves, long catchupTombstones, long catchupTTL,
            long catchupDuration) {
            this.index = index;
            this.sizeBeforeCompaction = sizeBeforeCompaction;
            this.sizeAfterCompaction = sizeAfterCompaction;
            this.keyCount = keyCount;
            this.removeCount = removeCount;
            this.tombstoneCount = tombstoneCount;
            this.ttlCount = ttlCount;
            this.duration = duration;
            this.catchupKeys = catchupKeys;
            this.catchupRemoves = catchupRemoves;
            this.catchupTombstones = catchupTombstones;
            this.catchupTTL = catchupTTL;
            this.catchupDuration = catchupDuration;
        }

    }
}
