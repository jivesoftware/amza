package com.jivesoftware.os.amza.api.ring;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class RingHostNGTest {

    @Test
    public void test() throws Exception {

        RingHost ringHost = new RingHost("dc", "rk", "h", 100);

        RingHost fromBytes = RingHost.fromBytes(ringHost.toBytes());
        Assert.assertEquals(ringHost, fromBytes);



    }

}
