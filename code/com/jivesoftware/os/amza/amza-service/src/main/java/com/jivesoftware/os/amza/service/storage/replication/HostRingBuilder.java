package com.jivesoftware.os.amza.service.storage.replication;

import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collection;

public class HostRingBuilder {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public HostRing build(RingHost serviceHost, Collection<RingHost> ringHosts) {
        ArrayList<RingHost> ring = new ArrayList<>(ringHosts);
        int rootIndex = -1;
        int index = 0;
        for (RingHost host : ring) {
            if (host.equals(serviceHost)) {
                rootIndex = index;
                break;
            }
            index++;
        }
        if (rootIndex == -1) {
            LOG.warn("serviceHost: " + serviceHost + " is not a memeber of the ring.");
            return new HostRing(new RingHost[0], new RingHost[0]);
        }

        ArrayList<RingHost> above = new ArrayList<>();
        ArrayList<RingHost> below = new ArrayList<>();
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
        return new HostRing(above.toArray(new RingHost[above.size()]), below.toArray(new RingHost[below.size()]));
    }
}
