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
package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.storage.FstMarshaller;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import de.ruedigermoeller.serialization.FSTConfiguration;

public class BinaryRowMarshaller implements RowMarshaller<byte[]> {

    private static final FstMarshaller FST_MARSHALLER = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());

    static {
        FST_MARSHALLER.registerSerializer(BinaryRow.class, new FSTBinaryRowMarshaller());
    }

    public BinaryRowMarshaller() {
    }

    @Override
    public byte[] toRow(long orderId, RowIndexKey k, RowIndexValue v) throws Exception {
        return FST_MARSHALLER.serialize(new BinaryRow(orderId,
                k.getKey(),
                v.getTimestamp(),
                v.getTombstoned(),
                v.getValue()));
    }

    @Override
    public boolean fromRow(byte[] row, RowScan rowStream) throws Exception {
        BinaryRow binaryRow = FST_MARSHALLER.deserialize(row, BinaryRow.class);
        return rowStream.row(binaryRow.transactionId, new RowIndexKey(binaryRow.key),
                new RowIndexValue(binaryRow.value, binaryRow.timestamp, binaryRow.tombstone));
    }
}
