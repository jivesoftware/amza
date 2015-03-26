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
package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.service.storage.RowStoreUpdates;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableInt;

public class AmzaRegion {

    private final OrderIdProvider orderIdProvider;
    private final RegionName regionName;
    private final RegionStore regionStore;

    public AmzaRegion(OrderIdProvider orderIdProvider, RegionName regionName, RegionStore regionStore) {
        this.regionName = regionName;
        this.orderIdProvider = orderIdProvider;
        this.regionStore = regionStore;
    }

    public RegionName getRegionName() {
        return regionName;
    }

    public WALKey set(WALKey key, byte[] value) throws Exception {
        if (value == null) {
            throw new IllegalStateException("Value cannot be null.");
        }
        RowStoreUpdates tx = regionStore.startTransaction(orderIdProvider.nextId());
        tx.add(key, value);
        tx.commit();
        return key;
    }

    public void set(Iterable<Entry<WALKey, byte[]>> entries) throws Exception {
        RowStoreUpdates tx = regionStore.startTransaction(orderIdProvider.nextId());
        for (Entry<WALKey, byte[]> e : entries) {
            WALKey k = e.getKey();
            byte[] v = e.getValue();
            if (v == null) {
                throw new IllegalStateException("Value cannot be null.");
            }
            tx.add(k, v);
        }
        tx.commit();
    }

    public byte[] get(WALKey key) throws Exception {
        WALValue got = regionStore.get(key);
        if (got == null) {
            return null;
        }
        if (got.getTombstoned()) {
            return null;
        }
        return got.getValue();
    }

    public List<byte[]> get(List<WALKey> keys) throws Exception {
        List<byte[]> values = new ArrayList<>();
        for (WALKey key : keys) {
            values.add(get(key));
        }
        return values;
    }

    public void get(Iterable<WALKey> keys, WALScan valuesStream) throws Exception {
        for (final WALKey key : keys) {
            WALValue rowIndexValue = regionStore.get(key);
            if (rowIndexValue != null && !rowIndexValue.getTombstoned()) {
                if (!valuesStream.row(-1, key, rowIndexValue)) {
                    return;
                }
            }
        }
    }

    public void scan(WALScan stream) throws Exception {
        regionStore.rowScan(stream);
    }

    public void rangeScan(WALKey from, WALKey to, WALScan stream) throws Exception {
        regionStore.rangeScan(from, to, stream);
    }

    public boolean remove(WALKey key) throws Exception {
        RowStoreUpdates tx = regionStore.startTransaction(orderIdProvider.nextId());
        tx.remove(key);
        tx.commit();
        return true;
    }

    public void remove(Iterable<WALKey> keys) throws Exception {
        RowStoreUpdates tx = regionStore.startTransaction(orderIdProvider.nextId());
        for (WALKey key : keys) {
            tx.remove(key);
        }
        tx.commit();
    }

    public void takeRowUpdatesSince(long transationId, RowStream rowStream) throws Exception {
        regionStore.takeRowUpdatesSince(transationId, rowStream);
    }

    //  Use for testing
    public boolean compare(final AmzaRegion amzaRegion) throws Exception {
        final MutableInt compared = new MutableInt(0);
        final MutableBoolean passed = new MutableBoolean(true);
        amzaRegion.regionStore.rowScan(new WALScan() {

            @Override
            public boolean row(long txid, WALKey key, WALValue value) {
                try {
                    compared.increment();

                    WALValue timestampedValue = regionStore.get(key);
                    String comparing = regionName.getRingName() + ":" + regionName.getRegionName()
                        + " to " + amzaRegion.regionName.getRingName() + ":" + amzaRegion.regionName.getRegionName() + "\n";

                    if (timestampedValue == null) {
                        System.out.println("INCONSISTENCY: " + comparing + " key:null"
                            + " != " + value.getTimestampId()
                            + "' \n" + timestampedValue + " vs " + value);
                        passed.setValue(false);
                        return false;
                    }
                    if (value.getTimestampId() != timestampedValue.getTimestampId()) {
                        System.out.println("INCONSISTENCY: " + comparing + " timstamp:'" + timestampedValue.getTimestampId()
                            + "' != '" + value.getTimestampId()
                            + "' \n" + timestampedValue + " vs " + value);
                        passed.setValue(false);
                        System.out.println("----------------------------------");

                        return false;
                    }
                    if (value.getTombstoned() != timestampedValue.getTombstoned()) {
                        System.out.println("INCONSISTENCY: " + comparing + " tombstone:" + timestampedValue.getTombstoned()
                            + " != '" + value.getTombstoned()
                            + "' \n" + timestampedValue + " vs " + value);
                        passed.setValue(false);
                        return false;
                    }
                    if (value.getValue() == null && timestampedValue.getValue() != null) {
                        System.out.println("INCONSISTENCY: " + comparing + " null values:" + timestampedValue.getTombstoned()
                            + " != '" + value.getTombstoned()
                            + "' \n" + timestampedValue + " vs " + value);
                        passed.setValue(false);
                        return false;
                    }
                    if (value.getValue() != null && !Arrays.equals(value.getValue(), timestampedValue.getValue())) {
                        System.out.println("INCONSISTENCY: " + comparing + " value:'" + timestampedValue.getValue()
                            + "' != '" + value.getValue()
                            + "' aClass:" + timestampedValue.getValue().getClass()
                            + "' bClass:" + value.getValue().getClass()
                            + "' \n" + timestampedValue + " vs " + value);
                        passed.setValue(false);
                        return false;
                    }
                    return true;
                } catch (Exception x) {
                    throw new RuntimeException("Failed while comparing", x);
                }
            }
        });

        System.out.println("region:" + amzaRegion.regionName.getRegionName() + " compared:" + compared + " keys");
        return passed.booleanValue();
    }
}
