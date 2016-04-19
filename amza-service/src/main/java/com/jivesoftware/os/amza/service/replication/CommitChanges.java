package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;

/**
 *
 * @author jonathan.colt
 */
interface CommitChanges {

    void commit(VersionedPartitionName versionedPartitionName, CommitTx commitTx) throws Exception;

}
