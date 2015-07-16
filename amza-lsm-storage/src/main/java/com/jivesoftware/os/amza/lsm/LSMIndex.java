package com.jivesoftware.os.amza.lsm;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.wal.KeyContainedStream;
import com.jivesoftware.os.amza.shared.wal.KeyValues;
import com.jivesoftware.os.amza.shared.wal.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.TxKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.TxKeyPointers;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeyValuePointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeys;
import com.jivesoftware.os.amza.shared.wal.WALTx;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author jonathan.colt
 */
public class LSMIndex implements WALIndex {

    private final VersionedPartitionName versionedPartitionName;
    private final PrimaryRowMarshaller<byte[]> primaryRowMarshaller;
    private final WALTx<LSMIndexLink> wal;

    private final AtomicReference<LSMIndexLink> index = new AtomicReference<>();

    public LSMIndex(VersionedPartitionName versionedPartitionName, PrimaryRowMarshaller<byte[]> primaryRowMarshaller, WALTx<LSMIndexLink> wal) {
        this.versionedPartitionName = versionedPartitionName;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.wal = wal;
    }

    public void open() throws Exception {
        index.set(wal.load(versionedPartitionName));
    }

    @Override
    public void updatedDescriptors(PrimaryIndexDescriptor primaryIndexDescriptor, SecondaryIndexDescriptor[] secondaryIndexDescriptors) {
    }

    @Override
    public void commit() throws Exception {
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void compact() throws Exception {
        Optional<WALTx.Compacted<LSMIndexLink>> compacted = wal.compact(0, 0, index.get(), false);
        if (compacted.isPresent()) {
            WALTx.CommittedCompacted<LSMIndexLink> commit = compacted.get().commit();
            index.set(commit.index);
        }
    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean delete() throws Exception {
        index.set(null);
        return wal.delete(false);
    }

    // WRITE
    @Override
    public boolean merge(TxKeyPointers pointers, MergeTxKeyPointerStream stream) throws Exception {
        /*
         private final LSMIndexLink fallbackIndex;
         private final ImmutableWALPointerBlockHydrator hydrator;
         private final TreeMap<byte[], LazyImmutableWALPointerBlock> prefixToWALPointerBlock;
         */

        List<byte[]> rawWALPointerBlocksInitialKeys = new ArrayList<>();
        List<byte[]> rawWALPointerBlocks = new ArrayList<>();
        
        long blockSize = 100;
        



        pointers.consume(new TxKeyPointerStream() {

            @Override
            public boolean stream(long txId, byte[] key, long timestamp, boolean tombstoned, long fp) throws Exception {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        });
        

//        long txId = 0; // TODO
//        wal.write((writer) -> {
//
//            return writer.write(txId, RowType.primary, (RawRowStream rawRowStream) -> {
//
//            }, null, null);
//        });

        return true;
    }

    // READ
    @Override
    public boolean getPointer(byte[] key, WALKeyPointerStream stream) throws Exception {
        LSMIndexLink got = index.get();
        if (got != null) {
            return got.getPointer(key, stream);
        } else {
            return stream.stream(key, -1, false, -1);
        }
    }

    @Override
    public boolean getPointers(WALKeys keys, WALKeyPointerStream stream) throws Exception {
        LSMIndexLink got = index.get();
        if (got != null) {
            return got.getPointers(keys, stream);
        } else {
            return keys.consume((key) -> {
                return stream.stream(key, -1, false, -1);
            });
        }
    }

    @Override
    public boolean getPointers(KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception {
        LSMIndexLink got = index.get();
        if (got != null) {
            return got.getPointers(keyValues, stream);
        } else {
            return keyValues.consume((byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) -> {
                return stream.stream(key, value, valueTimestamp, valueTombstoned, -1, false, -1);
            });
        }
    }

    @Override
    public boolean containsKeys(WALKeys keys, KeyContainedStream stream) throws Exception {
        LSMIndexLink got = index.get();
        if (got != null) {
            return got.containsKeys(keys, stream);
        } else {
            return keys.consume((key) -> {
                return stream.stream(key, false);
            });
        }
    }

    @Override
    public boolean isEmpty() throws Exception {
        LSMIndexLink got = index.get();
        if (got != null) {
            return got.isEmpty();
        } else {
            return true;
        }
    }

    @Override
    public long size() throws Exception {
        LSMIndexLink got = index.get();
        if (got != null) {
            return got.size();
        } else {
            return 0;
        }
    }

    @Override
    public boolean rangeScan(byte[] from, byte[] to, WALKeyPointerStream stream) throws Exception {
        LSMIndexLink got = index.get();
        if (got != null) {
            return got.rangeScan(from, to, stream);
        } else {
            return true;
        }
    }

    @Override
    public boolean rowScan(WALKeyPointerStream stream) throws Exception {
        LSMIndexLink got = index.get();
        if (got != null) {
            return got.rowScan(stream);
        } else {
            return true;
        }
    }

}
