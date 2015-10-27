package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.CommitTo;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;

/**
 *
 * @author jonathan.colt
 */
interface CommitChanges {

    void commit(VersionedPartitionName versionedPartitionName, CommitTx commitTx) throws Exception;

    interface CommitTx {

        RowsChanged tx(HighwaterStorage highwaterStorage, CommitTo commitTo) throws Exception;
    }
}
