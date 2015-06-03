package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.service.AmzaRingReader;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.shared.region.TxRegionStatus.Status;
import com.jivesoftware.os.amza.shared.region.VersionedRegionName;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class RegionComposter {

    private final RegionIndex regionIndex;
    private final RegionProvider regionProvider;
    private final AmzaRingReader amzaRingReader;
    private final RegionStripeProvider regionStripeProvider;
    private final RegionStatusStorage regionStatusStorage;

    public RegionComposter(RegionIndex regionIndex,
        RegionProvider regionProvider,
        AmzaRingReader amzaRingReader,
        RegionStatusStorage regionMemberStatusStorage,
        RegionStripeProvider regionStripeProvider) {

        this.regionIndex = regionIndex;
        this.regionProvider = regionProvider;
        this.amzaRingReader = amzaRingReader;
        this.regionStatusStorage = regionMemberStatusStorage;
        this.regionStripeProvider = regionStripeProvider;
    }

    public void compost() throws Exception {
        List<VersionedRegionName> composted = new ArrayList<>();
        regionStatusStorage.streamLocalState((regionName, ringMember, versionedStatus) -> {
            if (versionedStatus.status == Status.EXPUNGE) {
                RegionStripe regionStripe = regionStripeProvider.getRegionStripe(regionName);
                VersionedRegionName versionedRegionName = new VersionedRegionName(regionName, versionedStatus.version);
                if (regionStripe.expungeRegion(versionedRegionName)) {
                    regionIndex.remove(versionedRegionName);
                    composted.add(versionedRegionName);
                }
            } else if (!amzaRingReader.isMemberOfRing(regionName.getRingName()) || !regionProvider.hasRegion(regionName)) {
                regionStatusStorage.markForDisposal(new VersionedRegionName(regionName, versionedStatus.version), ringMember);
            }
            return true;
        });
        regionStatusStorage.expunged(composted);

    }

}
