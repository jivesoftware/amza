package com.jivesoftware.os.amza.api.wal;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.CompactableWALIndex;
import com.jivesoftware.os.amza.api.stream.RowType;
import java.io.File;
import java.util.concurrent.Callable;

/**
 * @author jonathan.colt
 */
public interface WALTx {

    <R> R tx(Tx<R> write) throws Exception;

    <R> R readFromTransactionId(long sinceTransactionId, WALReadWithOffset<R> readWithOffset) throws Exception;

    <R> R open(File baseKey, Tx<R> tx) throws Exception;

    <I extends CompactableWALIndex> I openIndex(File baseKey,
        WALIndexProvider<I> walIndexProvider,
        VersionedPartitionName partitionName,
        int maxValueSizeInIndex,
        int stripe) throws Exception;

    long length() throws Exception;

    void flush(boolean fsync) throws Exception;

    void delete(File baseKey) throws Exception;

    <I extends CompactableWALIndex> Compacted<I> compact(File fromBaseKey,
        File toBaseKey,
        RowType compactToRowType,
        long tombstoneTimestampId,
        long tombstoneVersion,
        long ttlTimestampId,
        long ttlVersion,
        long disposalVersion,
        I rowIndex,
        int stripe) throws Exception;

    interface EndOfMerge {

        byte[] endOfMerge(byte[] raw,
            long highestTxId,
            long oldestTimestamp,
            long oldestVersion,
            long oldestTombstonedTimestamp,
            long oldestTombstonedVersion,
            long keyCount,
            long fpOfLastLeap,
            long updatesSinceLeap) throws Exception;
    }

    void hackTruncation(int numBytes);

    interface Tx<R> {

        R tx(RowIO io) throws Exception;
    }

    interface WALReadWithOffset<R> {

        R read(long offset, WALReader reader) throws Exception;
    }

    interface Compacted<II> {

        CommittedCompacted<II> commit(EndOfMerge endOfMerge, Callable<Void> completedCompactCommit) throws Exception;
    }

    class CommittedCompacted<III> {

        public final III index;
        public final long sizeBeforeCompaction;
        public final long sizeAfterCompaction;
        public final long keyCount;
        public final long clobberCount;
        public final long tombstoneCount;
        public final long ttlCount;
        public final long disposalCount;

        public final long oldestTimestamp;
        public final long oldestVersion;
        public final long oldestTombstonedTimestamp;
        public final long oldestTombstonedVersion;

        public final long duration;

        public CommittedCompacted(III index,
            long sizeBeforeCompaction,
            long sizeAfterCompaction,
            long keyCount,
            long removeCount,
            long tombstoneCount,
            long ttlCount,
            long disposalCount,
            long oldestTimestamp,
            long oldestVersion,
            long oldestTombstonedTimestamp,
            long oldestTombstonedVersion,
            long duration) {
            this.index = index;
            this.sizeBeforeCompaction = sizeBeforeCompaction;
            this.sizeAfterCompaction = sizeAfterCompaction;
            this.keyCount = keyCount;
            this.clobberCount = removeCount;
            this.tombstoneCount = tombstoneCount;
            this.ttlCount = ttlCount;
            this.disposalCount = disposalCount;
            this.oldestTimestamp = oldestTimestamp;
            this.oldestVersion = oldestVersion;
            this.oldestTombstonedTimestamp = oldestTombstonedTimestamp;
            this.oldestTombstonedVersion = oldestTombstonedVersion;
            this.duration = duration;
        }

        @Override
        public String toString() {
            return "CommittedCompacted{"
                + "index=" + index
                + ", sizeBeforeCompaction=" + sizeBeforeCompaction
                + ", sizeAfterCompaction=" + sizeAfterCompaction
                + ", keyCount=" + keyCount
                + ", clobberCount=" + clobberCount
                + ", tombstoneCount=" + tombstoneCount
                + ", ttlCount=" + ttlCount
                + ", disposalCount=" + disposalCount
                + ", duration=" + duration
                + '}';
        }
    }
}
