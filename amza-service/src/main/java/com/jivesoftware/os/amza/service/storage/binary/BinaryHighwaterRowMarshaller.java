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
package com.jivesoftware.os.amza.service.storage.binary;

import com.jivesoftware.os.amza.service.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALHighwater.RingMemberHighwater;
import java.util.ArrayList;
import java.util.List;

public class BinaryHighwaterRowMarshaller implements HighwaterRowMarshaller<byte[]> {

    @Override
    public byte[] toBytes(WALHighwater highwater) throws Exception {
        HeapFiler filer = new HeapFiler();
        for (RingMemberHighwater rmh : highwater.ringMemberHighwater) {
            UIO.writeBoolean(filer, true, "hasMore");
            UIO.writeByteArray(filer, rmh.ringMember.toBytes(), "ringMember");
            UIO.writeLong(filer, rmh.transactionId, "txId");
        }
        UIO.writeBoolean(filer, false, "hasMore");
        return filer.getBytes();
    }

    @Override
    public WALHighwater fromBytes(byte[] row) throws Exception {
        HeapFiler filer = new HeapFiler(row);
        List<RingMemberHighwater> rmh = new ArrayList<>();
        while (UIO.readBoolean(filer, "hasMore")) {
            rmh.add(new RingMemberHighwater(RingMember.fromBytes(UIO.readByteArray(filer, "ringMember")), UIO.readLong(filer, "txId")));
        }
        return new WALHighwater(rmh);
    }
}
