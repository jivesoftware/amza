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

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.service.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import java.util.ArrayList;
import java.util.List;

public class BinaryHighwaterRowMarshaller implements HighwaterRowMarshaller<byte[]> {

    @Override
    public byte[] toBytes(WALHighwater highwater) throws Exception {
        byte[] lengthBuffer = new byte[4];
        HeapFiler filer = new HeapFiler(sizeInBytes(highwater));
        for (RingMemberHighwater rmh : highwater.ringMemberHighwater) {
            UIO.writeByte(filer, (byte) 1, "hasMore");
            UIO.writeByteArray(filer, rmh.ringMember.toBytes(), "ringMember", lengthBuffer);
            UIO.writeLong(filer, rmh.transactionId, "txId");
        }
        UIO.writeByte(filer, (byte) 0, "hasMore");
        return filer.getBytes();
    }

    @Override
    public int sizeInBytes(WALHighwater highwater) {
        int length = 0;
        for (RingMemberHighwater rmh : highwater.ringMemberHighwater) {
            length += 1 + 4 + rmh.ringMember.sizeInBytes() + 8;
        }
        length += 1;
        return length;
    }

    @Override
    public WALHighwater fromBytes(byte[] row) throws Exception {
        HeapFiler filer = HeapFiler.fromBytes(row, row.length);
        List<RingMemberHighwater> rmh = new ArrayList<>();
        byte[] intLongBuffer = new byte[8];
        while (UIO.readBoolean(filer, "hasMore")) {
            rmh.add(new RingMemberHighwater(RingMember.fromBytes(UIO.readByteArray(filer, "ringMember", intLongBuffer)),
                UIO.readLong(filer, "txId", intLongBuffer)));
        }
        return new WALHighwater(rmh);
    }
}
