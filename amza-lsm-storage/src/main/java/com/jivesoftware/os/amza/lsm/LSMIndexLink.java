package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.shared.scan.CompactableWALIndex;
import com.jivesoftware.os.amza.shared.wal.KeyContainedStream;
import com.jivesoftware.os.amza.shared.wal.KeyValues;
import com.jivesoftware.os.amza.shared.wal.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.TxKeyPointers;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeyValuePointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeys;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author jonathan.colt
 */
public class LSMIndexLink implements CompactableWALIndex {

    private final LSMIndexLink fallbackIndex;
    private final ImmutableWALPointerBlockHydrator hydrator;
    private final TreeMap<byte[], LazyImmutableWALPointerBlock> prefixToWALPointerBlock;

    public LSMIndexLink(LSMIndexLink fallbackIndex,
        ImmutableWALPointerBlockHydrator hydrator,
        TreeMap<byte[], LazyImmutableWALPointerBlock> prefixToWALPointerBlock) {
        this.fallbackIndex = fallbackIndex;
        this.hydrator = hydrator;
        this.prefixToWALPointerBlock = prefixToWALPointerBlock;
    }

    @Override
    public boolean getPointer(byte[] key, WALKeyPointerStream stream) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean merge(TxKeyPointers pointers, MergeTxKeyPointerStream stream) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void commit() throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

     @Override
    public boolean isEmpty() throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }


    public boolean getPointerFoo(byte[] key, WALKeyPointerStream stream) throws Exception {
        Map.Entry<byte[], LazyImmutableWALPointerBlock> floorEntry = prefixToWALPointerBlock.floorEntry(key);
        if (floorEntry != null) {
            return floorEntry.getValue().getPointer(hydrator, key, fallbackIndex == null ? stream : (key1, timestamp, tombstoned, fp) -> {
                if (fp == -1) {
                    return fallbackIndex.getPointer(key, stream);
                } else {
                    return stream.stream(key, timestamp, tombstoned, fp);
                }
            });
        } else {
            return stream.stream(key, -1, false, -1);
        }
    }


    public boolean getPointers(WALKeys keys, WALKeyPointerStream stream) throws Exception {
        return keys.consume((byte[] key) -> {
            Map.Entry<byte[], LazyImmutableWALPointerBlock> floorEntry = prefixToWALPointerBlock.floorEntry(key);
            if (floorEntry != null) {
                return floorEntry.getValue().getPointer(hydrator, key, fallbackIndex == null ? stream : (key1, timestamp, tombstoned, fp) -> {
                    if (fp == -1) {
                        return fallbackIndex.getPointer(key, stream);
                    } else {
                        return stream.stream(key, timestamp, tombstoned, fp);
                    }
                });
            } else {
                return stream.stream(key, -1, false, -1);
            }
        });
    }

    public boolean getPointers(KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public boolean containsKeys(WALKeys keys, KeyContainedStream stream) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }



    public long size() throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public boolean rangeScan(byte[] from, byte[] to, WALKeyPointerStream stream) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public boolean rowScan(WALKeyPointerStream stream) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }



}
