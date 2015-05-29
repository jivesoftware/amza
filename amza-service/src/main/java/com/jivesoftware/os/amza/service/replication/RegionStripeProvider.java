package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 * @author jonathan.colt
 */
public class RegionStripeProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RegionStripe systemStripe;
    private final RegionStripe[] deltaStripes;

    public RegionStripeProvider(RegionStripe systemStripe, RegionStripe[] stripes) {
        this.systemStripe = systemStripe;
        this.deltaStripes = stripes;
    }

    public RegionStripe getSystemRegionStripe() {
        return systemStripe;
    }

    public RegionStripe getRegionStripe(RegionName regionName) throws Exception {
        if (regionName.isSystemRegion()) {
            return systemStripe;
        }
        return deltaStripes[Math.abs(regionName.hashCode()) % deltaStripes.length];
    }

    public void removeRegion(VersionedRegionName versionedRegionName) throws Exception {
        if (!versionedRegionName.getRegionName().isSystemRegion()) {
            deltaStripes[Math.abs(versionedRegionName.getRegionName().hashCode()) % deltaStripes.length].removeRegion(versionedRegionName);
        }
    }
}
