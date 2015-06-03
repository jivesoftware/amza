/*
 * Copyright 2015 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.shared.region.RegionName;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author jonathan.colt
 */
public class RecentRegionTakers {

    private final ConcurrentHashMap<RegionName, Takers> recentRegionTakers = new ConcurrentHashMap<>();

    public void took(RingMember ringMember, RingHost ringHost, RegionName regionName) {
        recentRegionTakers.computeIfAbsent(regionName, (t) -> new Takers()).took(ringMember, ringHost);
    }

    public Collection<RingHost> recentTakers(RegionName regionName) {
        Takers takers = recentRegionTakers.get(regionName);
        if (takers == null) {
            return Collections.emptyList();
        }
        return takers.recentTakers();
    }

    static class Takers {

        private final LinkedHashMap<RingMember, RingHost> recentTakers = new LinkedHashMap<>(16, 0.75f, true);

        synchronized public void took(RingMember ringMember, RingHost ringHost) {
            recentTakers.put(ringMember, ringHost);
        }

        synchronized Collection<RingHost> recentTakers() {
            return new ArrayList<>(recentTakers.values());
        }
    }

}
