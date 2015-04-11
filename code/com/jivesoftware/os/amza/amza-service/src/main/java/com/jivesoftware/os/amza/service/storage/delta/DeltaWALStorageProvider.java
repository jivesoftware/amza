package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.WALStorage;

/**
 *
 * @author jonathan.colt
 */
public interface DeltaWALStorageProvider {

    StripeWALStorage getDeltaWALStorage(RegionName regionName, WALStorage storage);
}
