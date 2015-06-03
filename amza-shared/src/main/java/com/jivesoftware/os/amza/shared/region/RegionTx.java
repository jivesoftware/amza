package com.jivesoftware.os.amza.shared.region;

import com.jivesoftware.os.amza.shared.region.TxRegionStatus.Status;

/**
 *
 * @author jonathan.colt
 */
public interface RegionTx<R> {

    R tx(VersionedRegionName versionedRegionName, Status regionStatus) throws Exception;
}
