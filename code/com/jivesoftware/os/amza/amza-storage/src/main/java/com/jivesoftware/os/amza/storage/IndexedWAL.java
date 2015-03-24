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
package com.jivesoftware.os.amza.storage;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALIndex;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReader;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALTx;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.WALWriter;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class IndexedWAL implements WALStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int numTickleMeElmaphore = 1024; // TODO config

    private final RegionName regionName;
    private final OrderIdProvider orderIdProvider;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final WALTx walTx;
    private final WALReplicator walReplicator;
    private final AtomicReference<WALIndex> walIndex = new AtomicReference<>(null);
    private final Object oneWriterAtATimeLock = new Object();
    private final Semaphore tickleMeElmaphore = new Semaphore(numTickleMeElmaphore, true);
    private final int maxUpdatesBetweenMarkers;

    private final AtomicLong updateCount = new AtomicLong(0);
    private final AtomicLong newCount = new AtomicLong(0);
    private final AtomicLong clobberCount = new AtomicLong(0);

    public IndexedWAL(RegionName regionName,
        OrderIdProvider orderIdProvider,
        RowMarshaller<byte[]> rowMarshaller,
        WALTx walTx,
        WALReplicator walReplicator,
        int maxUpdatesBetweenMarkers) {

        this.regionName = regionName;
        this.orderIdProvider = orderIdProvider;
        this.rowMarshaller = rowMarshaller;
        this.walTx = walTx;
        this.walReplicator = walReplicator;
        this.maxUpdatesBetweenMarkers = maxUpdatesBetweenMarkers;
    }

    public RegionName getRegionName() {
        return regionName;
    }

    @Override
    public void compactTombstone(long removeTombstonedOlderThanTimestampId) throws Exception {

        if ((clobberCount.get() + 1) / (newCount.get() + 1) > 2) { // TODO expose to config
            Optional<WALTx.Compacted> compact = walTx.compact(regionName, removeTombstonedOlderThanTimestampId, walIndex.get());

            if (compact.isPresent()) {

                tickleMeElmaphore.acquire(numTickleMeElmaphore);
                try {
                    WALIndex compactedRowsIndex = compact.get().getCompactedWALIndex();
                    walIndex.set(compactedRowsIndex);
                    newCount.set(0);
                    clobberCount.set(0);

                } finally {
                    tickleMeElmaphore.release(numTickleMeElmaphore);
                }
            }

        }
    }

    @Override
    public void load() throws Exception {
        tickleMeElmaphore.acquire(numTickleMeElmaphore);
        try {
            walIndex.compareAndSet(null, walTx.load(regionName));

            final AtomicLong rowsVisited = new AtomicLong(maxUpdatesBetweenMarkers);
            walTx.read(new WALTx.WALRead<Void>() {

                @Override
                public Void read(WALReader rowReader) throws Exception {
                    rowReader.reverseScan(new WALReader.Stream() {
                        @Override
                        public boolean row(long rowPointer, byte rowType, byte[] row) throws Exception {
                            if (rowType == WALWriter.SYSTEM_VERSION_1) {
                                long[] keyNewCountClobberCount = FilerIO.bytesLongs(row);
                                newCount.set(keyNewCountClobberCount[1]);
                                clobberCount.set(keyNewCountClobberCount[2]);
                                return false;
                            }
                            return rowsVisited.decrementAndGet() >= 0;
                        }
                    });
                    return null;
                }
            });

            walTx.write(new WALTx.WALWrite<Void>() {

                @Override
                public Void write(WALWriter writer) throws Exception {
                    writeMarker(writer);
                    return null;
                }

            });
        } finally {
            tickleMeElmaphore.release(numTickleMeElmaphore);
        }
    }

    private void writeMarker(WALWriter rowWriter) throws Exception {
        rowWriter.write(Collections.nCopies(1, WALWriter.SYSTEM_VERSION_1),
            Collections.singletonList(FilerIO.longsBytes(new long[] {
                0, // Key
                newCount.get(),
                clobberCount.get()
            })), true);
        updateCount.set(0);
    }

    @Override
    public RowsChanged update(WALScanable updates) throws Exception {
        final AtomicLong oldestAppliedTimestamp = new AtomicLong(Long.MAX_VALUE);
        final NavigableMap<WALKey, WALValue> apply = new TreeMap<>();
        final NavigableMap<WALKey, WALValue> removes = new TreeMap<>();
        final Multimap<WALKey, WALValue> clobbers = ArrayListMultimap.create();

        final List<WALKey> keys = new ArrayList<>();
        final List<WALValue> values = new ArrayList<>();
        updates.rowScan(new WALScan() {
            @Override
            public boolean row(long transactionId, WALKey key, WALValue update) throws Exception {
                keys.add(key);
                values.add(update);
                return true;
            }
        });

        tickleMeElmaphore.acquire();
        try {
            WALIndex wali = walIndex.get();
            synchronized (oneWriterAtATimeLock) {
                List<WALValue> currentValues = wali.get(keys);
                for (int i = 0; i < keys.size(); i++) {
                    WALKey key = keys.get(i);
                    WALValue current = currentValues.get(i);
                    WALValue update = values.get(i);
                    if (current == null) {
                        apply.put(key, update);
                        if (oldestAppliedTimestamp.get() > update.getTimestampId()) {
                            oldestAppliedTimestamp.set(update.getTimestampId());
                        }
                    } else if (current.getTimestampId() < update.getTimestampId()) {
                        apply.put(key, update);
                        if (oldestAppliedTimestamp.get() > update.getTimestampId()) {
                            oldestAppliedTimestamp.set(update.getTimestampId());
                        }
                        WALValue value = hydrateRowIndexValue(current);
                        clobbers.put(key, value);
                        if (update.getTombstoned() && !current.getTombstoned()) {
                            removes.put(key, value);
                        }
                    }
                }

                if (walReplicator != null && !apply.isEmpty()) {
                    walReplicator.replicate(new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers));
                }

                final NavigableMap<WALKey, byte[]> keyToRowPointer = new TreeMap<>();

                if (apply.isEmpty()) {
                    return new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers);
                } else {
                    final long transactionId = (orderIdProvider == null) ? 0 : orderIdProvider.nextId();
                    walTx.write(new WALTx.WALWrite<Void>() {
                        @Override
                        public Void write(WALWriter rowWriter) throws Exception {

                            List<WALKey> keys = new ArrayList<>();
                            List<byte[]> rawRows = new ArrayList<>();
                            for (Map.Entry<WALKey, WALValue> e : apply.entrySet()) {
                                WALKey key = e.getKey();
                                WALValue value = e.getValue();
                                keys.add(key);
                                rawRows.add(rowMarshaller.toRow(transactionId, key, value));
                            }
                            List<byte[]> rowPointers = rowWriter.write(Collections.nCopies(rawRows.size(), WALWriter.VERSION_1), rawRows, true);
                            for (int i = 0; i < rowPointers.size(); i++) {
                                keyToRowPointer.put(keys.get(i), rowPointers.get(i));
                            }

                            if (updateCount.addAndGet(keys.size()) > maxUpdatesBetweenMarkers) {
                                writeMarker(rowWriter);
                            }
                            return null;
                        }
                    });

                    for (Map.Entry<WALKey, WALValue> entry : apply.entrySet()) {
                        WALKey key = entry.getKey();
                        WALValue value = entry.getValue();
                        byte[] rowPointer = keyToRowPointer.get(key);
                        WALValue rowValue = new WALValue(rowPointer, value.getTimestampId(), value.getTombstoned());
                        WALValue got = wali.get(Collections.singletonList(key)).get(0);
                        if (got == null) {
                            wali.put(Collections.singletonList(new SimpleEntry<>(key, rowValue)));
                            newCount.incrementAndGet();
                        } else if (got.getTimestampId() < value.getTimestampId()) {
                            // TODO add a counter around this and leverage to avoid lame compactions
                            wali.put(Collections.singletonList(new SimpleEntry<>(key, rowValue)));
                            clobberCount.incrementAndGet();
                        }
                    }

                    return new RowsChanged(regionName, oldestAppliedTimestamp.get(), apply, removes, clobbers);
                }
            }
        } finally {
            tickleMeElmaphore.release();
        }
    }

    @Override
    public void rowScan(final WALScan walScan) throws Exception {
        tickleMeElmaphore.acquire();
        try {
            WALIndex rowsIndex = walIndex.get();
            rowsIndex.rowScan(new WALScan() {
                @Override
                public boolean row(long transactionId, WALKey key, WALValue value) throws Exception {
                    return walScan.row(transactionId, key, hydrateRowIndexValue(value));
                }
            });
        } finally {
            tickleMeElmaphore.release();
        }
    }

    @Override
    public void rangeScan(final WALKey from, final WALKey to, final WALScan walScan) throws Exception {
        tickleMeElmaphore.acquire();
        try {
            WALIndex wali = walIndex.get();
            wali.rangeScan(from, to, new WALScan() {
                @Override
                public boolean row(long transactionId, WALKey key, WALValue value) throws Exception {
                    return walScan.row(transactionId, key, hydrateRowIndexValue(value));
                }
            });
        } finally {
            tickleMeElmaphore.release();
        }
    }

    @Override
    public WALValue get(WALKey key) throws Exception {
        return get(Collections.singletonList(key)).get(0);
    }

    @Override
    public List<WALValue> get(List<WALKey> keys) throws Exception {
        List<WALValue> gots;
        tickleMeElmaphore.acquire();
        try {
            gots = walIndex.get().get(keys);
        } finally {
            tickleMeElmaphore.release();
        }
        return Lists.transform(gots, new Function<WALValue, WALValue>() {

            @Override
            public WALValue apply(WALValue input) {
                if (input == null) {
                    return null;
                }
                return hydrateRowIndexValue(input);
            }
        });
    }

    @Override
    public boolean containsKey(WALKey key) throws Exception {
        return containsKey(Collections.singletonList(key)).get(0);
    }

    @Override
    public List<Boolean> containsKey(List<WALKey> keys) throws Exception {
        tickleMeElmaphore.acquire();
        try {
            return walIndex.get().containsKey(keys);
        } finally {
            tickleMeElmaphore.release();
        }
    }

    private WALValue hydrateRowIndexValue(final WALValue rowIndexValue) {
        try {
            // TODO replace with a read only pool of rowReaders
            byte[] row = walTx.read(new WALTx.WALRead<byte[]>() {

                @Override
                public byte[] read(WALReader rowReader) throws Exception {
                    return rowReader.read(rowIndexValue.getValue());
                }
            });
            byte[] value = rowMarshaller.valueFromRow(row);
            return new WALValue(value,
                rowIndexValue.getTimestampId(),
                rowIndexValue.getTombstoned());
        } catch (Exception x) {
            throw new RuntimeException("Failed to hydrtate " + rowIndexValue, x);
        }
    }

    @Override
    public void takeRowUpdatesSince(final long sinceTransactionId, final WALScan rowStream) throws Exception {
        synchronized (oneWriterAtATimeLock) {
            final WALScan filteringRowStream = new WALScan() {

                @Override
                public boolean row(long transactionId, WALKey key, WALValue value) throws Exception {

                    if (transactionId > sinceTransactionId) {
                        if (!rowStream.row(transactionId, key, value)) {
                            return false;
                        }
                    }
                    return transactionId > sinceTransactionId;
                }
            };

            walTx.read(new WALTx.WALRead<Void>() {

                @Override
                public Void read(WALReader rowReader) throws Exception {
                    rowReader.reverseScan(new WALReader.Stream() {
                        @Override
                        public boolean row(long rowPointer, byte rowType, byte[] row) throws Exception {
                            if (rowType > 0) {
                                RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
                                return filteringRowStream.row(walr.getTransactionId(), walr.getKey(), walr.getValue());
                            }
                            return true;
                        }
                    });
                    return null;
                }
            });
        }
    }

}
