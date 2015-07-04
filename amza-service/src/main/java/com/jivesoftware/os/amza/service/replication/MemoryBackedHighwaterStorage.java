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

import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryBackedHighwaterStorage implements HighwaterStorage {

    private final ConcurrentHashMap<RingMember, ConcurrentHashMap<VersionedPartitionName, Long>> lastTransactionIds = new ConcurrentHashMap<>();

    @Override
    public void clearRing(RingMember member) {
        lastTransactionIds.remove(member);
    }

    @Override
    public boolean expunge(VersionedPartitionName versionedPartitionName) throws Exception {
        for (ConcurrentHashMap<VersionedPartitionName, Long> got : lastTransactionIds.values()) {
            got.remove(versionedPartitionName);
        }
        return true;
    }

    @Override
    public WALHighwater getPartitionHighwater(VersionedPartitionName versionedPartitionName) throws Exception {
        List<WALHighwater.RingMemberHighwater> highwaters = new ArrayList<>();
        for (RingMember ringMember : lastTransactionIds.keySet()) {
            Long highwaterTxId = lastTransactionIds.get(ringMember).get(versionedPartitionName);
            if (highwaterTxId != null) {
                highwaters.add(new WALHighwater.RingMemberHighwater(ringMember, highwaterTxId));
            }
        }
        return new WALHighwater(highwaters);
    }

    @Override
    public void setIfLarger(RingMember ringMember, VersionedPartitionName versionedPartitionName, int update, long highWatermark) {
        ConcurrentHashMap<VersionedPartitionName, Long> lastPartitionTransactionIds = lastTransactionIds.computeIfAbsent(ringMember, (RingMember t) -> {
            return new ConcurrentHashMap<>();
        });

        lastPartitionTransactionIds.merge(versionedPartitionName, highWatermark, (current, candidate) -> {
            return current == null ? update: Math.max(current, candidate);
        });
    }

    @Override
    public void clear(RingMember member, VersionedPartitionName versionedPartitionName) {
        ConcurrentHashMap<VersionedPartitionName, Long> lastPartitionTransactionIds = lastTransactionIds.get(member);
        if (lastPartitionTransactionIds != null) {
            lastPartitionTransactionIds.remove(versionedPartitionName);
        }
    }

    @Override
    public Long get(RingMember member, VersionedPartitionName versionedPartitionName) {
        ConcurrentHashMap<VersionedPartitionName, Long> lastPartitionTransactionIds = lastTransactionIds.get(member);
        if (lastPartitionTransactionIds == null) {
            return -1L;
        }
        Long got = lastPartitionTransactionIds.get(versionedPartitionName);
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
    public void flush(Callable<Void> preFlush) throws Exception {
        if (preFlush != null) {
            preFlush.call();
        }
    }

}
