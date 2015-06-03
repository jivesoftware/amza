package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.shared.TxRegionStatus.Status;

/**
 *
 * @author jonathan.colt
 */
public interface RegionTx<R> {

    R tx(VersionedRegionName versionedRegionName, Status regionStatus) throws Exception;
}
