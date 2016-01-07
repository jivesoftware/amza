package com.jivesoftware.os.amza.api.wal;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.CompactableWALIndex;
import com.jivesoftware.os.amza.api.stream.RowType;

/**
 * @author jonathan.colt
 */
public interface WALTx {

    <R> R write(WALWrite<R> write) throws Exception;

    <R> R read(WALRead<R> read) throws Exception;

    <R> R readFromTransactionId(long sinceTransactionId, WALReadWithOffset<R> readWithOffset) throws Exception;

    void validateAndRepair() throws Exception;

    <I extends CompactableWALIndex> I load(WALIndexProvider<I> walIndexProvider,
        VersionedPartitionName partitionName,
        int maxUpdatesBetweenIndexCommitMarker) throws Exception;

    long length() throws Exception;

    void flush(boolean fsync) throws Exception;

    void delete() throws Exception;

    <I extends CompactableWALIndex> Optional<Compacted<I>> compact(RowType compactToRowType,
        long removeTombstonedOlderThanTimestampId,
        long ttlTimestampId,
        I rowIndex,
        boolean force) throws Exception;

    void hackTruncation(int numBytes);

    interface WALWrite<R> {

        R write(WALWriter writer) throws Exception;
    }

    interface WALRead<R> {

        R read(WALReader reader) throws Exception;
    }

    interface WALReadWithOffset<R> {

        R read(long offset, WALReader reader) throws Exception;
    }

    interface Compacted<II> {

        CommittedCompacted<II> commit() throws Exception;
    }

    class CommittedCompacted<III> {

        public final III index;
        public final long sizeBeforeCompaction;
        public final long sizeAfterCompaction;
        public final long keyCount;
        public final long clobberCount;
        public final long tombstoneCount;
        public final long ttlCount;
        public final long duration;
        public final long catchupKeyCount;
        public final long catchupClobberCount;
        public final long catchupTombstoneCount;
        public final long catchupTTLCount;
        public final long catchupDuration;

        public CommittedCompacted(III index, long sizeBeforeCompaction, long sizeAfterCompaction, long keyCount, long removeCount,
            long tombstoneCount, long ttlCount, long duration, long catchupKeyCount, long catchupClobberCount, long catchupTombstoneCount, long catchupTTLCount,
            long catchupDuration) {
            this.index = index;
            this.sizeBeforeCompaction = sizeBeforeCompaction;
            this.sizeAfterCompaction = sizeAfterCompaction;
            this.keyCount = keyCount;
            this.clobberCount = removeCount;
            this.tombstoneCount = tombstoneCount;
            this.ttlCount = ttlCount;
            this.duration = duration;
            this.catchupKeyCount = catchupKeyCount;
            this.catchupClobberCount = catchupClobberCount;
            this.catchupTombstoneCount = catchupTombstoneCount;
            this.catchupTTLCount = catchupTTLCount;
            this.catchupDuration = catchupDuration;
        }

    }
}
