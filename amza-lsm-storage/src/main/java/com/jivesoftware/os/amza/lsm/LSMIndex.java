package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.wal.KeyContainedStream;
import com.jivesoftware.os.amza.shared.wal.KeyValues;
import com.jivesoftware.os.amza.shared.wal.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.TxKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.TxKeyPointers;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeyValuePointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeys;
import com.jivesoftware.os.amza.shared.wal.WALTx;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class LSMIndex implements WALIndex {

    private final VersionedPartitionName versionedPartitionName;
    private final WALTx<LSMIndex> wal;

    public LSMIndex(VersionedPartitionName versionedPartitionName, WALTx<LSMIndex> wal) {
        this.versionedPartitionName = versionedPartitionName;
        this.wal = wal;
    }

    public void load() throws Exception {

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

    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean delete() throws Exception {
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
        return true;
    }

    @Override
    public boolean getPointers(WALKeys keys, WALKeyPointerStream stream) throws Exception {
        return true;

    }

    @Override
    public boolean getPointers(KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception {
        return true;

    }

    @Override
    public boolean containsKeys(WALKeys keys, KeyContainedStream stream) throws Exception {
        return true;

    }

    @Override
    public boolean isEmpty() throws Exception {

        return true;
    }

    @Override
    public long size() throws Exception {
        return 0;
    }

    @Override
    public boolean rangeScan(byte[] from, byte[] to, WALKeyPointerStream stream) throws Exception {
        return true;

    }

    @Override
    public boolean rowScan(WALKeyPointerStream stream) throws Exception {
        return true;

    }

}
