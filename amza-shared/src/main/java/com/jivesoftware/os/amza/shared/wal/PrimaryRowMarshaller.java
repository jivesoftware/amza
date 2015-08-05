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

import com.jivesoftware.os.amza.shared.stream.FpKeyValueStream;
import com.jivesoftware.os.amza.shared.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.shared.stream.UnprefixedTxKeyValueStream;

public interface PrimaryRowMarshaller<R> {

    R toRow(byte[] key, byte[] value, long timestamp, boolean tombstoned) throws Exception;

    interface FpRows {

        boolean consume(FpRowStream fpRowStream) throws Exception;
    }

    interface FpRowStream {

        boolean stream(long fp, byte[] row) throws Exception;
    }

    interface TxFpRows {

        boolean consume(TxFpRowStream txFpRowStream) throws Exception;
    }

    interface TxFpRowStream {

        boolean stream(long txId, long fp, byte[] row) throws Exception;
    }

    boolean fromRows(FpRows fpRows, FpKeyValueStream fpKeyValueStream) throws Exception;

    boolean fromRows(TxFpRows txFpRows, TxKeyValueStream txKeyValueStream) throws Exception;

    boolean fromRows(TxFpRows txFpRows, UnprefixedTxKeyValueStream txKeyValueStream) throws Exception;

    boolean fromRows(TxFpRows txFpRows, WALKey.TxFpKeyValueEntryStream<byte[]> txFpKeyValueEntryStream) throws Exception;

    byte[] valueFromRow(R row) throws Exception;

    long timestampFromRow(R row) throws Exception;

    boolean tombstonedFromRow(R row) throws Exception;
}
