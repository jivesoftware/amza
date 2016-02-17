package com.jivesoftware.os.amza.service.take;

import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.take.TakeRingCoordinator.VersionedRing;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 *
 */
public class VersionedRingTest {

    @Test
    public void testCategories() {
        VersionedRing versionedRing = VersionedRing.compute(
            new RingTopology(-1, -1, Lists.newArrayList(memberAndHost("a"), memberAndHost("b"), memberAndHost("c"), memberAndHost("d")), 0));
        assertNull(versionedRing.getCategory(member("a")));
        assertEquals(1, versionedRing.getCategory(member("b")).intValue());
        assertEquals(1, versionedRing.getCategory(member("c")).intValue());
        assertEquals(2, versionedRing.getCategory(member("d")).intValue());

        versionedRing = VersionedRing.compute(
            new RingTopology(-1, -1, Lists.newArrayList(memberAndHost("a"), memberAndHost("b"), memberAndHost("c"), memberAndHost("d")), 1));
        assertEquals(2, versionedRing.getCategory(member("a")).intValue());
        assertNull(versionedRing.getCategory(member("b")));
        assertEquals(1, versionedRing.getCategory(member("c")).intValue());
        assertEquals(1, versionedRing.getCategory(member("d")).intValue());

        versionedRing = VersionedRing.compute(
            new RingTopology(-1, -1, Lists.newArrayList(memberAndHost("a"), memberAndHost("b"), memberAndHost("c"), memberAndHost("d")), 2));
        assertEquals(1, versionedRing.getCategory(member("a")).intValue());
        assertEquals(2, versionedRing.getCategory(member("b")).intValue());
        assertNull(versionedRing.getCategory(member("c")));
        assertEquals(1, versionedRing.getCategory(member("d")).intValue());

        versionedRing = VersionedRing.compute(
            new RingTopology(-1, -1, Lists.newArrayList(memberAndHost("a"), memberAndHost("b"), memberAndHost("c"), memberAndHost("d")), 3));
        assertEquals(1, versionedRing.getCategory(member("a")).intValue());
        assertEquals(1, versionedRing.getCategory(member("b")).intValue());
        assertEquals(2, versionedRing.getCategory(member("c")).intValue());
        assertNull(versionedRing.getCategory(member("d")));
    }

    private RingMemberAndHost memberAndHost(String name) {
        return new RingMemberAndHost(member(name), new RingHost("datacenter", "rack", name, 1));
    }

    private RingMember member(String name) {
        return new RingMember(name);
    }
}
