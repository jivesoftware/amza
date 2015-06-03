package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.shared.region.RegionName;

/**
 *
 * @author jonathan.colt
 */
public interface AmzaRegionAPIProvider {

    AmzaRegionAPI getRegion(RegionName regionName) throws Exception;
}
