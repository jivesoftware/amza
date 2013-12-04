/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.storage.index;

import com.jivesoftware.os.amza.shared.BinaryTimestampedValue;
import com.jivesoftware.os.amza.shared.EntryStream;
import com.jivesoftware.os.amza.shared.TableIndex;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.storage.FstMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRow;
import com.jivesoftware.os.amza.storage.binary.FSTBinaryRowMarshaller;
import com.jivesoftware.os.amza.storage.chunks.ChunkFiler;
import com.jivesoftware.os.amza.storage.chunks.SubFiler;
import com.jivesoftware.os.amza.storage.chunks.UIO;
import de.ruedigermoeller.serialization.FSTConfiguration;
import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

public class MapDBValueChunkedTableIndex implements TableIndex {

    private static final FstMarshaller FST_MARSHALLER = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());

    static {
        FST_MARSHALLER.registerSerializer(BinaryRow.class, new FSTBinaryRowMarshaller());
    }

    private final DB db;
    private final BTreeMap<TableIndexKey, BinaryTimestampedValue> treeMap;
    private final ChunkFiler chunkFiler;
    private final TableName tableName;

    public MapDBValueChunkedTableIndex(String mapName, File workingDirectory, TableName tableName) throws Exception {
        this.db = DBMaker.newDirectMemoryDB()
                .closeOnJvmShutdown()
                .make();
        this.treeMap = db.getTreeMap(mapName);
        this.chunkFiler = ChunkFiler.factory(workingDirectory, "values-" + tableName.getTableName());
        this.tableName = tableName;
    }

    @Override
    public void flush() {
        db.commit();
    }

    @Override
    public <E extends Throwable> void entrySet(EntryStream<E> entryStream) {
        for (Entry<TableIndexKey, BinaryTimestampedValue> e : treeMap.entrySet()) {
            LazyLoadingTimestampValue lazyLoadingTimestampValue = new LazyLoadingTimestampValue(e.getValue().getValue(), e.getValue().getTimestamp(),
                    e.getValue().getTombstoned());
            try {
                if (!entryStream.stream(e.getKey(), lazyLoadingTimestampValue)) {
                    break;
                }
            } catch (Throwable t) {
                throw new RuntimeException("Failed while streaming entry set.", t);
            }
        }
    }

    @Override
    public boolean isEmpty() {
        return treeMap.isEmpty();
    }

    @Override
    public boolean containsKey(TableIndexKey key) {
        return treeMap.containsKey(key);
    }

    @Override
    public BinaryTimestampedValue get(TableIndexKey key) {
        BinaryTimestampedValue got = treeMap.get(key);
        if (got == null) {
            return null;
        }
        return new LazyLoadingTimestampValue(got.getValue(), got.getTimestamp(), got.getTombstoned());
    }

    @Override
    public BinaryTimestampedValue put(TableIndexKey key,
            BinaryTimestampedValue value) {
        byte[] valueAsBytes;
        try {
            valueAsBytes = FST_MARSHALLER.serialize(value.getValue());
        } catch (IOException x) {
            throw new RuntimeException("Failed to serialize " + value.getValue().getClass(), x);
        }
        long chunkId;
        try {
            chunkId = chunkFiler.newChunk(valueAsBytes.length);
            SubFiler filer = chunkFiler.getFiler(chunkId);
            filer.setBytes(valueAsBytes);
            filer.flush();
        } catch (Exception x) {
            throw new RuntimeException("Failed to save value to chuck filer. " + value.getValue().getClass(), x);
        }

        BinaryTimestampedValue basicTimestampedValue = new BinaryTimestampedValue(UIO.longBytes(chunkId), value.getTimestamp(), value.getTombstoned());
        BinaryTimestampedValue had = treeMap.put(key, basicTimestampedValue);
        if (had == null) {
            return null;
        }
        return new LazyLoadingTimestampValue(had.getValue(), had.getTimestamp(), had.getTombstoned());
    }

    @Override
    public BinaryTimestampedValue remove(TableIndexKey key) {
        BinaryTimestampedValue removed = treeMap.remove(key);
        if (removed == null) {
            return null;
        }
        // TODO should we load the value and delete it from the chunk filer?
        return new LazyLoadingTimestampValue(removed.getValue(), removed.getTimestamp(), removed.getTombstoned());
    }

    @Override
    public void clear() {
        // TODO should we clean out the chunkfiler
        treeMap.clear();
    }

    public class LazyLoadingTimestampValue extends BinaryTimestampedValue {

        public LazyLoadingTimestampValue(byte[] value,
                long timestamp,
                boolean tombstoned) {
            super(value, 0, tombstoned);
        }

        @Override
        public byte[] getValue() {
            try {
                SubFiler filer = chunkFiler.getFiler(UIO.bytesLong(super.getValue()));
                return filer.toBytes();
            } catch (Exception x) {
                throw new RuntimeException("Unable to read value from chunkFiler for chunkId:" + UIO.bytesLong(super.getValue()), x);
            }
        }
    }
}
