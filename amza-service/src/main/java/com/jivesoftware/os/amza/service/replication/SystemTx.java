package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;

/**
 *
 * @author jonathan.colt
 */
public interface SystemTx<R> {

    R tx(int stripe, HighwaterStorage highwaterStorage, VersionedAquarium versionedAquarium) throws Exception;

}
