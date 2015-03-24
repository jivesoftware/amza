package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.shared.RingHost;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class HostRingBuilderNGTest {

    public HostRingBuilderNGTest() {
    }

    @Test
    public void testBuild() {
        HostRingBuilder builder = new HostRingBuilder();
        HostRing hostRing = builder.build(new RingHost("a", 1), Arrays.asList(new RingHost("a", 1), new RingHost("a", 2)));
        Assert.assertTrue(Arrays.equals(new RingHost[]{new RingHost("a", 2)}, hostRing.getAboveRing()), "Above2a:" + Arrays.toString(hostRing.getAboveRing()));
        Assert.assertTrue(Arrays.equals(new RingHost[]{new RingHost("a", 2)}, hostRing.getBelowRing()), "Below2a:" + Arrays.toString(hostRing.getBelowRing()));

        hostRing = builder.build(new RingHost("a", 2), Arrays.asList(new RingHost("a", 1), new RingHost("a", 2)));
        Assert.assertTrue(Arrays.equals(new RingHost[]{new RingHost("a", 1)}, hostRing.getAboveRing()), "Above2b:" + Arrays.toString(hostRing.getAboveRing()));
        Assert.assertTrue(Arrays.equals(new RingHost[]{new RingHost("a", 1)}, hostRing.getBelowRing()), "Below2b:" + Arrays.toString(hostRing.getBelowRing()));

        hostRing = builder.build(new RingHost("a", 1), Arrays.asList(new RingHost("a", 1), new RingHost("a", 2), new RingHost("a", 3)));
        Assert.assertTrue(Arrays.equals(new RingHost[]{new RingHost("a", 3), new RingHost("a", 2)}, hostRing.getAboveRing()),
            "Above3a:" + Arrays.toString(hostRing.getAboveRing()));
        Assert.assertTrue(Arrays.equals(new RingHost[]{new RingHost("a", 2), new RingHost("a", 3)}, hostRing.getBelowRing()),
            "Below3a:" + Arrays.toString(hostRing.getBelowRing()));


        hostRing = builder.build(new RingHost("a", 2), Arrays.asList(new RingHost("a", 1), new RingHost("a", 2), new RingHost("a", 3)));
        Assert.assertTrue(Arrays.equals(new RingHost[]{new RingHost("a", 1), new RingHost("a", 3)}, hostRing.getAboveRing()),
            "Above3b:" + Arrays.toString(hostRing.getAboveRing()));
        Assert.assertTrue(Arrays.equals(new RingHost[]{new RingHost("a", 3), new RingHost("a", 1)}, hostRing.getBelowRing()),
            "Below3b:" + Arrays.toString(hostRing.getBelowRing()));

        hostRing = builder.build(new RingHost("a", 3), Arrays.asList(new RingHost("a", 1), new RingHost("a", 2), new RingHost("a", 3)));
        Assert.assertTrue(Arrays.equals(new RingHost[]{new RingHost("a", 2), new RingHost("a", 1)}, hostRing.getAboveRing()),
            "Above4b:" + Arrays.toString(hostRing.getAboveRing()));
        Assert.assertTrue(Arrays.equals(new RingHost[]{new RingHost("a", 1), new RingHost("a", 2)}, hostRing.getBelowRing()),
            "Below4b:" + Arrays.toString(hostRing.getBelowRing()));


    }
}
