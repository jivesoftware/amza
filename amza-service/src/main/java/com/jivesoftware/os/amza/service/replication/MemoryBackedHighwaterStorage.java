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

import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.amza.shared.HighwaterStorage;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingMember;
import com.jivesoftware.os.amza.shared.WALHighwater;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class MemoryBackedHighwaterStorage implements HighwaterStorage {

    private final ConcurrentHashMap<RingMember, ConcurrentHashMap<RegionName, Long>> lastTransactionIds = new ConcurrentHashMap<>();

    @Override
    public void clearRing(RingMember member) {
        lastTransactionIds.remove(member);
    }

    @Override
    public WALHighwater getRegionHighwater(RegionName regionName) throws Exception {
        List<WALHighwater.RingMemberHighwater> highwaters = new ArrayList<>();
        for (RingMember ringMember : lastTransactionIds.keySet()) {
            Long highwaterTxId = lastTransactionIds.get(ringMember).get(regionName);
            if (highwaterTxId != null) {
                highwaters.add(new WALHighwater.RingMemberHighwater(ringMember, highwaterTxId));
            }
        }
        return new WALHighwater(highwaters);
    }

    @Override
    public void setIfLarger(RingMember ringMember, RegionName regionName, int update, long highWatermark) {
        ConcurrentHashMap<RegionName, Long> lastRegionTransactionIds = lastTransactionIds.get(ringMember);
        if (lastRegionTransactionIds == null) {
            lastRegionTransactionIds = new ConcurrentHashMap<>();
            lastTransactionIds.put(ringMember, lastRegionTransactionIds);
        }
        Long got = lastRegionTransactionIds.get(regionName);
        if (got == null) {
            Long had = lastRegionTransactionIds.putIfAbsent(regionName, highWatermark);
            if (had < highWatermark) {
                lastRegionTransactionIds.put(regionName, had);
            }
        } else {
            if (got < highWatermark) {
                lastRegionTransactionIds.put(regionName, highWatermark);
            }
        }
    }

    @Override
    public void clear(RingMember member, RegionName regionName) {
        ConcurrentHashMap<RegionName, Long> lastRegionTransactionIds = lastTransactionIds.get(member);
        if (lastRegionTransactionIds != null) {
            lastRegionTransactionIds.remove(regionName);
        }
    }

    @Override
    public Long get(RingMember member, RegionName regionName) {
        ConcurrentHashMap<RegionName, Long> lastRegionTransactionIds = lastTransactionIds.get(member);
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
        return "MemoryBackedHighwaterStorage{" + "lastTransactionIds=" + lastTransactionIds + '}';
    }

    @Override
    public void flush(ListMultimap<RingMember, RegionName> flush) throws Exception {
    }

}
