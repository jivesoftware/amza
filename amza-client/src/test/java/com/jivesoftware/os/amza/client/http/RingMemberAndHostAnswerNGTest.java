package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class RingMemberAndHostAnswerNGTest {

    @Test
    public void testGetRingMemberAndHost() {

        RingMember ringMember = new RingMember("m1");
        RingHost ringHost = new RingHost("dc1", "rack1", "localhost", 1);
        RingMemberAndHost ringMemberAndHost = new RingMemberAndHost(ringMember, ringHost);
        RingMemberAndHostAnswer<String> rmaha = new RingMemberAndHostAnswer<>(ringMemberAndHost, "bla");
        Assert.assertEquals("bla", rmaha.getAnswer());
        Assert.assertEquals(ringMemberAndHost, rmaha.getRingMemberAndHost());
    }

}
