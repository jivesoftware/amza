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

import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;

/**
 * Uses 42bits for time, 19bits for writerId, 2bits for orderId and 1 bit for add vs removed.
 */
public class AmzaChangeIdPacker implements IdPacker {

    @Override
    public int bitsPrecisionOfOrderId() {
        return 2;
    }

    @Override
    public int bitsPrecisionOfTimestamp() {
        return 42;
    }

    @Override
    public int bitsPrecisionOfWriterId() {
        return 19;
    }

    @Override
    public long pack(long timestamp, int writerId, int orderId) {
        long id = (timestamp & 0x1FFFFFFFFFFL) << bitsPrecisionOfWriterId() + bitsPrecisionOfOrderId() + 1;
        id |= ((writerId & 0x1FF) << bitsPrecisionOfOrderId() + 1);
        id |= ((orderId & 0xFFF) << 1);
        return id;
    }

    @Override
    public long[] unpack(long packedId) {
        long packed = packedId;
        long time = (packed & (0x1FFFFFFFFFFL << bitsPrecisionOfWriterId() + bitsPrecisionOfOrderId() + 1))
            >>> bitsPrecisionOfWriterId() + bitsPrecisionOfOrderId() + 1;
        int writer = (int) ((packed & (0x1FF << bitsPrecisionOfOrderId() + 1)) >>> bitsPrecisionOfOrderId() + 1);
        int order = (int) ((packed & (0xFFF << 1)) >>> 1);
        return new long[]{time, writer, order};
    }
}
