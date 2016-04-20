package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.scan.CommitTo;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;

/**
 *
 * @author jonathan.colt
 */
interface CommitTx {

    RowsChanged tx(HighwaterStorage highwaterStorage, VersionedAquarium versionedAquarium, CommitTo commitTo) throws Exception;

}
