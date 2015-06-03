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

import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.ring.RingNeighbors;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.NavigableMap;

public class HostRingBuilder {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public RingNeighbors build(RingMember localMember, NavigableMap<RingMember, RingHost> nodes) {
        ArrayList<Entry<RingMember, RingHost>> ring = new ArrayList<>(nodes.entrySet());
        int rootIndex = -1;
        int index = 0;
        for (Entry<RingMember, RingHost> node : ring) {
            if (node.getKey().equals(localMember)) {
                rootIndex = index;
                break;
            }
            index++;
        }
        if (rootIndex == -1) {
            LOG.warn("serviceHost: " + localMember + " is not a member of the ring.");
            return new RingNeighbors((Entry<RingMember, RingHost>[]) new AbstractMap.SimpleEntry[0],
                (Entry<RingMember, RingHost>[]) new AbstractMap.SimpleEntry[0]);
        }

        ArrayList<Entry<RingMember, RingHost>> above = new ArrayList<>();
        ArrayList<Entry<RingMember, RingHost>> below = new ArrayList<>();
        int aboveI = rootIndex - 1;
        int belowI = rootIndex + 1;
        for (int i = 1; i < ring.size(); i++) {
            if (aboveI < 0) {
                aboveI = ring.size() - 1;
            }
            if (belowI >= ring.size()) {
                belowI = 0;
            }
            above.add(ring.get(aboveI));
            below.add(ring.get(belowI));
            aboveI--;
            belowI++;
        }
        return new RingNeighbors(
            (Entry<RingMember, RingHost>[]) above.toArray(new Entry[above.size()]),
            (Entry<RingMember, RingHost>[]) below.toArray(new Entry[below.size()]));

    }
}
