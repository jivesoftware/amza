package com.jivesoftware.os.amza.client;

import com.jivesoftware.os.amza.shared.AmzaRegionAPI;
import com.jivesoftware.os.amza.shared.region.RegionName;

/**
 *
 * @author jonathan.colt
 */
public interface AmzaRegionAwaitQuorum {

    void await(RegionName regionName, AmzaRegionAPI.TakeQuorum takeQuorum, int desiredTakeQuorum, long toMillis) throws Exception;

}
