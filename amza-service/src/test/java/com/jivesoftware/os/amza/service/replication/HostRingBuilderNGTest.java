package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.ring.RingNeighbors;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class HostRingBuilderNGTest {

    public HostRingBuilderNGTest() {
    }

    Entry<RingMember, RingHost> forge(String name, int port) {
        return new AbstractMap.SimpleEntry<>(new RingMember(name), new RingHost(name, port));
    }

    NavigableMap<RingMember, RingHost> forge(Entry<RingMember, RingHost>... es) {
        NavigableMap<RingMember, RingHost> map = new TreeMap<>();
        for (Entry<RingMember, RingHost> e : es) {
            map.put(e.getKey(), e.getValue());
        }
        return map;
    }

    @Test
    public void testBuild() {
        HostRingBuilder builder = new HostRingBuilder();
        RingNeighbors ringNeighbors = builder.build(new RingMember("a"), forge(forge("a", 1), forge("b", 2)));
        Assert.assertTrue(Arrays.equals(new Entry[]{forge("b", 2)}, ringNeighbors.getAboveRing()), "Above2a:" + Arrays.toString(ringNeighbors.getAboveRing()));
        Assert.assertTrue(Arrays.equals(new Entry[]{forge("b", 2)}, ringNeighbors.getBelowRing()), "Below2a:" + Arrays.toString(ringNeighbors.getBelowRing()));

        ringNeighbors = builder.build(new RingMember("b"), forge(forge("a", 1), forge("b", 2)));
        Assert.assertTrue(Arrays.equals(new Entry[]{forge("a", 1)}, ringNeighbors.getAboveRing()), "Above2b:" + Arrays.toString(ringNeighbors.getAboveRing()));
        Assert.assertTrue(Arrays.equals(new Entry[]{forge("a", 1)}, ringNeighbors.getBelowRing()), "Below2b:" + Arrays.toString(ringNeighbors.getBelowRing()));

        ringNeighbors = builder.build(new RingMember("a"), forge(forge("a", 1), forge("b", 2), forge("c", 3)));
        Assert.assertTrue(Arrays.equals(new Entry[]{forge("c", 3), forge("b", 2)}, ringNeighbors.getAboveRing()),
            "Above3a:" + Arrays.toString(ringNeighbors.getAboveRing()));
        Assert.assertTrue(Arrays.equals(new Entry[]{forge("b", 2), forge("c", 3)}, ringNeighbors.getBelowRing()),
            "Below3a:" + Arrays.toString(ringNeighbors.getBelowRing()));

        ringNeighbors = builder.build(new RingMember("b"), forge(forge("a", 1), forge("b", 2), forge("c", 3)));
        Assert.assertTrue(Arrays.equals(new Entry[]{forge("a", 1), forge("c", 3)}, ringNeighbors.getAboveRing()),
            "Above3b:" + Arrays.toString(ringNeighbors.getAboveRing()));
        Assert.assertTrue(Arrays.equals(new Entry[]{forge("c", 3), forge("a", 1)}, ringNeighbors.getBelowRing()),
            "Below3b:" + Arrays.toString(ringNeighbors.getBelowRing()));

        ringNeighbors = builder.build(new RingMember("c"), forge(forge("a", 1), forge("b", 2), forge("c", 3)));
        Assert.assertTrue(Arrays.equals(new Entry[]{forge("b", 2), forge("a", 1)}, ringNeighbors.getAboveRing()),
            "Above4b:" + Arrays.toString(ringNeighbors.getAboveRing()));
        Assert.assertTrue(Arrays.equals(new Entry[]{forge("a", 1), forge("b", 2)}, ringNeighbors.getBelowRing()),
            "Below4b:" + Arrays.toString(ringNeighbors.getBelowRing()));

    }

}
