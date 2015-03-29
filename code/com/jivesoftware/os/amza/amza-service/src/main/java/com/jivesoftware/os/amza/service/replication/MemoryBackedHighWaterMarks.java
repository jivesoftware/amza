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
package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.shared.HighwaterMarks;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryBackedHighWaterMarks implements HighwaterMarks {

    private final ConcurrentHashMap<RingHost, ConcurrentHashMap<RegionName, Long>> lastTransactionIds = new ConcurrentHashMap<>();

    @Override
    public void clearRing(RingHost ringHost) {
        lastTransactionIds.remove(ringHost);
    }

    @Override
    public void set(RingHost ringHost, RegionName regionName, int update, long highWatermark) {
        ConcurrentHashMap<RegionName, Long> lastRegionTransactionIds = lastTransactionIds.get(ringHost);
        if (lastRegionTransactionIds == null) {
            lastRegionTransactionIds = new ConcurrentHashMap<>();
            lastTransactionIds.put(ringHost, lastRegionTransactionIds);
        }
        lastRegionTransactionIds.put(regionName, highWatermark);
    }

    @Override
    public void clear(RingHost ringHost, RegionName regionName) {
        ConcurrentHashMap<RegionName, Long> lastRegionTransactionIds = lastTransactionIds.get(ringHost);
        if (lastRegionTransactionIds != null) {
            lastRegionTransactionIds.remove(regionName);
        }
    }

    @Override
    public Long get(RingHost ringHost, RegionName regionName) {
        ConcurrentHashMap<RegionName, Long> lastRegionTransactionIds = lastTransactionIds.get(ringHost);
        if (lastRegionTransactionIds == null) {
            return -1L;
        }
        Long got = lastRegionTransactionIds.get(regionName);
        if (got == null) {
            return -1L;
        }
        return got;
    }

    @Override
    public String toString() {
        return "MemoryBackedHighWaterMarks{" + "lastTransactionIds=" + lastTransactionIds + '}';
    }

}
