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
package com.jivesoftware.os.amza.client.http;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 *
 * @author jonathan.colt
 */
public class Ring {

    private static final Random RANDOM = new Random(); // Random number generator
    private final int leaderIndex;
    final RingMemberAndHost[] members;

    public Ring(int leaderIndex, RingMemberAndHost[] members) {
        this.leaderIndex = leaderIndex;
        this.members = members;
    }

    public RingMemberAndHost leader() {
        return (leaderIndex == -1) ? null : members[leaderIndex];
    }

    public RingMemberAndHost[] actualRing() {
        return members;
    }

    public RingMemberAndHost[] orderedRing(List<RingMember> membersInOrder) {
        List<RingMemberAndHost> ordered = Lists.newArrayListWithCapacity(members.length);
        if (membersInOrder == null) {
            if (leaderIndex >= 0) {
                ordered.add(members[leaderIndex]);
            }
            for (int i = 0; i < members.length; i++) {
                if (i != leaderIndex) {
                    ordered.add(members[i]);
                }
            }
        } else {
            Map<RingMember, RingMemberAndHost> memberHosts = Maps.newHashMapWithExpectedSize(members.length);
            for (RingMemberAndHost member : members) {
                memberHosts.put(member.ringMember, member);
            }
            for (RingMember ringMember : membersInOrder) {
                RingMemberAndHost ringMemberAndHost = memberHosts.remove(ringMember);
                if (ringMemberAndHost != null) {
                    ordered.add(ringMemberAndHost);
                }
            }
            for (RingMemberAndHost ringMemberAndHost : memberHosts.values()) {
                ordered.add(ringMemberAndHost);
            }
        }
        return ordered.toArray(new RingMemberAndHost[0]);
    }

    public RingMemberAndHost[] leaderlessRing() {
        if (leaderIndex == -1) {
            return Arrays.copyOf(members, members.length);
        } else {
            RingMemberAndHost[] memberAndHosts = new RingMemberAndHost[members.length - 1];
            System.arraycopy(members, 0, memberAndHosts, 0, leaderIndex);
            System.arraycopy(members, leaderIndex + 1, memberAndHosts, leaderIndex, members.length - 1 - leaderIndex);
            return memberAndHosts;
        }
    }

    public RingMemberAndHost[] randomizeRing() {
        RingMemberAndHost[] array = Arrays.copyOf(members, members.length);
        for (int i = 0; i < array.length; i++) {
            int randomPosition = RANDOM.nextInt(array.length);
            RingMemberAndHost temp = array[i];
            array[i] = array[randomPosition];
            array[randomPosition] = temp;
        }
        return array;
    }

}
