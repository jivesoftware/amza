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
package com.jivesoftware.os.amza.api.wal;

import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedTxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.FpKeyValueStream;

public interface PrimaryRowMarshaller {

    byte[] toRow(RowType rowType, byte[] key, byte[] value, long timestamp, boolean tombstoned, long version) throws Exception;

    int sizeInBytes(int pkSizeInBytes, int valueSizeInBytes);

    interface FpRows {

        boolean consume(FpRowStream fpRowStream) throws Exception;
    }

    interface FpRowStream {

        boolean stream(long fp, RowType rowType, byte[] row) throws Exception;
    }

    interface TxFpRows {

        boolean consume(TxFpRowStream txFpRowStream) throws Exception;
    }

    interface TxFpRowStream {

        boolean stream(long txId, long fp, RowType rowType, byte[] row) throws Exception;
    }

    byte[] convert(RowType fromType, byte[] row, RowType toType) throws Exception;

    boolean fromRows(FpRows fpRows, FpKeyValueStream fpKeyValueStream) throws Exception;

    boolean fromRows(TxFpRows txFpRows, TxKeyValueStream txKeyValueStream) throws Exception;

    boolean fromRows(TxFpRows txFpRows, UnprefixedTxKeyValueStream txKeyValueStream) throws Exception;

    boolean fromRows(TxFpRows txFpRows, WALKey.TxFpKeyValueEntryStream<byte[]> txFpKeyValueEntryStream) throws Exception;

    byte[] valueFromRow(RowType rowType, byte[] row, int offset) throws Exception;

    long timestampFromRow(byte[] row, int offset) throws Exception;

    boolean tombstonedFromRow(byte[] row, int offset) throws Exception;

    long versionFromRow(byte[] row, int offset) throws Exception;
}
