package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.shared.RegionName;

/**
 * @author jonathan.colt
 */
public class RegionStripeProvider {

    private final RegionStripe systemStripe;
    private final RegionIndex regionIndex;
    private final RegionStripe[] deltaStripes;

    public RegionStripeProvider(RegionStripe systemStripe, RegionIndex regionIndex, RegionStripe[] stripes) {
        this.systemStripe = systemStripe;
        this.regionIndex = regionIndex;
        this.deltaStripes = stripes;
    }

    public RegionStripe getSystemRegionStripe() {
        return systemStripe;
    }

    public Optional<RegionStripe> getRegionStripe(RegionName regionName) throws Exception {
        if (regionName.isSystemRegion()) {
            return Optional.of(systemStripe);
        }
        if (regionIndex.exists(regionName)) {
            return Optional.of(deltaStripes[Math.abs(regionName.hashCode()) % deltaStripes.length]);
        } else {
            return Optional.absent();
        }
    }

}
