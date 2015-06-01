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
import com.jivesoftware.os.amza.shared.RingMember;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import com.jivesoftware.os.amza.shared.WALHighwater;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryBackedHighwaterStorage implements HighwaterStorage {

    private final ConcurrentHashMap<RingMember, ConcurrentHashMap<VersionedRegionName, Long>> lastTransactionIds = new ConcurrentHashMap<>();

    @Override
    public void clearRing(RingMember member) {
        lastTransactionIds.remove(member);
    }

    @Override
    public boolean expunge(VersionedRegionName versionedRegionName) throws Exception {
        for (ConcurrentHashMap<VersionedRegionName, Long> got:lastTransactionIds.values()) {
            got.remove(versionedRegionName);
        }
        return true;
    }

    @Override
    public WALHighwater getRegionHighwater(VersionedRegionName versionedRegionName) throws Exception {
        List<WALHighwater.RingMemberHighwater> highwaters = new ArrayList<>();
        for (RingMember ringMember : lastTransactionIds.keySet()) {
            Long highwaterTxId = lastTransactionIds.get(ringMember).get(versionedRegionName);
            if (highwaterTxId != null) {
                highwaters.add(new WALHighwater.RingMemberHighwater(ringMember, highwaterTxId));
            }
        }
        return new WALHighwater(highwaters);
    }

    @Override
    public void setIfLarger(RingMember ringMember, VersionedRegionName versionedRegionName, int update, long highWatermark) {
        ConcurrentHashMap<VersionedRegionName, Long> lastRegionTransactionIds = lastTransactionIds.get(ringMember);
        if (lastRegionTransactionIds == null) {
            lastRegionTransactionIds = new ConcurrentHashMap<>();
            lastTransactionIds.put(ringMember, lastRegionTransactionIds);
        }
        Long got = lastRegionTransactionIds.get(versionedRegionName);
        if (got == null) {
            Long had = lastRegionTransactionIds.putIfAbsent(versionedRegionName, highWatermark);
            if (had < highWatermark) {
                lastRegionTransactionIds.put(versionedRegionName, had);
            }
        } else {
            if (got < highWatermark) {
                lastRegionTransactionIds.put(versionedRegionName, highWatermark);
            }
        }
    }

    @Override
    public void clear(RingMember member, VersionedRegionName versionedRegionName) {
        ConcurrentHashMap<VersionedRegionName, Long> lastRegionTransactionIds = lastTransactionIds.get(member);
        if (lastRegionTransactionIds != null) {
            lastRegionTransactionIds.remove(versionedRegionName);
        }
    }

    @Override
    public Long get(RingMember member, VersionedRegionName versionedRegionName) {
        ConcurrentHashMap<VersionedRegionName, Long> lastRegionTransactionIds = lastTransactionIds.get(member);
        if (lastRegionTransactionIds == null) {
            return -1L;
        }
        Long got = lastRegionTransactionIds.get(versionedRegionName);
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
    public void flush(ListMultimap<RingMember, VersionedRegionName> flush) throws Exception {
    }

}
