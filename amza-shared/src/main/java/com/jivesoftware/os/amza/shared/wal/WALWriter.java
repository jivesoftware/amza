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

import com.jivesoftware.os.amza.shared.scan.RowType;

public interface WALWriter {

    int write(long txId, RowType rowType, RawRows rows, IndexableKeys indexableKeys, TxKeyPointerFpStream stream) throws Exception;

    long writeSystem(byte[] row) throws Exception;

    long writeHighwater(byte[] row) throws Exception;

    long getEndOfLastRow() throws Exception;

    interface RawRows {
        boolean consume(RawRowStream stream) throws Exception;
    }

    interface RawRowStream {
        boolean stream(byte[] row) throws Exception;
    }

    interface IndexableKeys {
        boolean consume(IndexableKeyStream stream) throws Exception;
    }

    interface IndexableKeyStream {
        boolean stream(byte[] prefix, byte[] key, long valueTimestamp, boolean valueTombstoned) throws Exception;
    }

    interface TxKeyPointerFpStream {
        boolean stream(long txId, byte[] prefix, byte[] key, long valueTimestamp, boolean valueTombstoned, long fp) throws Exception;
    }
}
