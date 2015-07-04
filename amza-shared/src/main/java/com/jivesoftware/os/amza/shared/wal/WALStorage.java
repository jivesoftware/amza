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
package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.RangeScannable;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import java.util.List;

public interface WALStorage extends RangeScannable<WALValue> {

    void load() throws Exception;

    void flush(boolean fsync) throws Exception;

    boolean delete(boolean ifEmpty) throws Exception;

    RowsChanged update(boolean useUpdateTxId, Commitable<WALValue> rowUpdates) throws Exception;

    WALValue get(WALKey key);

    WALValue[] get(WALKey[] keys) throws Exception;

    boolean containsKey(WALKey key) throws Exception;

    WALPointer[] getPointers(WALKey[] consumableKeys, List<WALValue> values) throws Exception;

    List<Boolean> containsKey(List<WALKey> keys) throws Exception;

    /**
     * Takes from the WAL starting no later than the requested transactionId.
     *
     * @param transactionId the desired starting transactionId
     * @param rowUpdates the callback stream
     * @return true if the end of the WAL was reached, otherwise false
     * @throws Exception if an error occurred
     */
    boolean takeRowUpdatesSince(final long transactionId, RowStream rowUpdates) throws Exception;

    /**
     * Takes from the WAL starting no later than the requested transactionId.
     *
     * @param transactionId the desired starting transactionId
     * @param scan the callback scan
     * @return true if the end of the WAL was reached, otherwise false
     * @throws Exception if an error occurred
     */
    boolean takeFromTransactionId(long transactionId, Highwaters watermarks, Scan<WALValue> scan) throws Exception;

    boolean compactableTombstone(long removeTombstonedOlderTimestampId, long ttlTimestampId) throws Exception;

    long compactTombstone(long removeTombstonedOlderTimestampId, long ttlTimestampId) throws Exception;

    void updatedStorageDescriptor(WALStorageDescriptor walStorageDescriptor) throws Exception;

    long count() throws Exception;

    long highestTxId();

    boolean expunge() throws Exception;
}
