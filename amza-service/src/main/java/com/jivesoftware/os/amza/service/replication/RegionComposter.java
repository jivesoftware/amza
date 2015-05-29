package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.shared.TxRegionStatus.Status;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class RegionComposter {

    private final RegionIndex regionIndex;
    private final RegionStripeProvider regionStripeProvider;
    private final RegionStatusStorage regionStatusStorage;

    public RegionComposter(RegionIndex regionIndex,
        RegionStatusStorage regionMemberStatusStorage,
        RegionStripeProvider regionStripeProvider) {
        
        this.regionIndex = regionIndex;
        this.regionStatusStorage = regionMemberStatusStorage;
        this.regionStripeProvider = regionStripeProvider;
    }

    public void compost() throws Exception {
        List<VersionedRegionName> composted = new ArrayList<>();
        regionStatusStorage.streamLocalState((regionName, ringMember, versionedStatus) -> {
            if (versionedStatus.status == Status.DISPOSE) {
                RegionStripe regionStripe = regionStripeProvider.getRegionStripe(regionName);
                VersionedRegionName versionedRegionName = new VersionedRegionName(regionName, versionedStatus.version);
                if (regionStripe.removeRegion(versionedRegionName)) {
                    regionIndex.remove(versionedRegionName);
                    composted.add(versionedRegionName);
                }
            }
            return true;
        });
        regionStatusStorage.disposed(composted);

    }

}
