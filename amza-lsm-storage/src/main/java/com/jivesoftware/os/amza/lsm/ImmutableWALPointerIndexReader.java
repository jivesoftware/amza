package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.wal.WALReader;
import com.jivesoftware.os.amza.shared.wal.WALTx;
import com.jivesoftware.os.amza.shared.wal.WALWriter;
import java.lang.ref.WeakReference;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 *
 * @author jonathan.colt
 */
public class ImmutableWALPointerIndexReader<I> {

    private final WALTx<I> walTx;
    private final long walPointerIndexFP;

    private WeakReference<TreeMap<byte[], ImmutableWALPSortedBlockReader<I>>> weakIndex;

    public ImmutableWALPointerIndexReader(WALTx<I> walTx, long walPointerIndexFP) {
        this.walTx = walTx;
        this.walPointerIndexFP = walPointerIndexFP;
    }

    public long getWalPointerIndexFP() {
        return walPointerIndexFP;
    }

    public static <I> ImmutableWALPointerIndexReader<I> write(WALTx<I> walTx, TreeMap<byte[], ImmutableWALPSortedBlockReader<I>> index) throws Exception {
        return walTx.write((WALWriter writer) -> {
            for (Entry<byte[], ImmutableWALPSortedBlockReader<I>> e : index.entrySet()) {
                //writer.write(txId, RowType.system, null, null, null);
            }
            return new ImmutableWALPointerIndexReader<>(walTx, -1);
        });

    }

    private TreeMap<byte[], ImmutableWALPSortedBlockReader<I>> hydrateIndex() throws Exception {
        TreeMap<byte[], ImmutableWALPSortedBlockReader<I>> got = weakIndex == null ? null : weakIndex.get();
        if (got == null) {
            got = walTx.read((WALReader reader) -> {
                byte[] rawKeys = reader.read(walPointerIndexFP);
                HeapFiler filer = new HeapFiler(rawKeys);
                int count = UIO.readInt(filer, "count");
                TreeMap<byte[], ImmutableWALPSortedBlockReader<I>> index = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
                for (int i = 0; i < count; i++) {
                    byte[] key = UIO.readByteArray(filer, "key");
                    long keysFP = UIO.readLong(filer, "keysFP");
                    long pointersFP = UIO.readLong(filer, "pointersFP");
                    index.put(key, new ImmutableWALPSortedBlockReader<>(walTx, keysFP, pointersFP));
                }
                return index;

            });
            weakIndex = new WeakReference<>(got);
        }
        return got;
    }
/*
    public boolean getPointer(byte[] key, WALKeyPointerStream stream) throws Exception {
        Map.Entry<byte[], ImmutableWALPSortedBlockReader<I>> floorEntry = hydrateIndex().floorEntry(key);
        if (floorEntry != null) {
            return floorEntry.getValue().getPointer(key, stream);
        } else {
            return stream.stream(key, -1, false, -1);
        }
    }

    public boolean getPointers(WALKeys keys, WALKeyPointerStream stream) throws Exception {
        return keys.consume((byte[] key) -> {
            Map.Entry<byte[], ImmutableWALPSortedBlockReader<I>> floorEntry = hydrateIndex().floorEntry(key);
            if (floorEntry != null) {
                return floorEntry.getValue().getPointer(key, stream);
            } else {
                return stream.stream(key, -1, false, -1);
            }
        });
    }

    public boolean getPointers(KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception {
        return keyValues.consume((byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) -> {
            Map.Entry<byte[], ImmutableWALPSortedBlockReader<I>> floorEntry = hydrateIndex().floorEntry(key);
            if (floorEntry != null) {
                return floorEntry.getValue().getPointer(key, value, valueTimestamp, valueTombstoned, stream);
            } else {
                return stream.stream(key, value, valueTimestamp, valueTombstoned, -1, false, -1);
            }
        });
    }

    public boolean containsKeys(WALKeys keys, KeyContainedStream stream) throws Exception {
        return keys.consume((byte[] key) -> {
            Map.Entry<byte[], ImmutableWALPSortedBlockReader<I>> floorEntry = hydrateIndex().floorEntry(key);
            if (floorEntry != null) {
                return floorEntry.getValue().containsKey(key, stream);
            } else {
                return stream.stream(key, false);
            }
        });
    }

    public long size() throws Exception {
        long size = 0;
        for (ImmutableWALPSortedBlockReader<I> value : hydrateIndex().values()) {
            size += value.size();
        }
        return size;
    }

    public boolean rangeScan(byte[] from, byte[] to, WALKeyPointerStream stream) throws Exception {
        NavigableMap<byte[], ImmutableWALPSortedBlockReader<I>> subMap = hydrateIndex().subMap(from, true, to, false);
        for (ImmutableWALPSortedBlockReader<I> value : subMap.values()) {
            if (!value.rangeScan(from, to, stream)) {
                return false;
            }
        }
        return true;
    }

    public boolean rowScan(WALKeyPointerStream stream) throws Exception {
        for (ImmutableWALPSortedBlockReader<I> value : hydrateIndex().values()) {
            if (!value.rowScan(stream)) {
                return false;
            }
        }
        return true;
    }*/
}
