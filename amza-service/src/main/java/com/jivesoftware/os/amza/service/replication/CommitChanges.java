package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.CommitTo;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;

/**
 *
 * @author jonathan.colt
 */
interface CommitChanges {

    boolean needsTxId(RingMember ringMember, long txId) throws Exception;

    void commit(CommitTx commitTx) throws Exception;

    interface CommitTx {

        RowsChanged tx(VersionedPartitionName versionedPartitionName, HighwaterStorage highwaterStorage, CommitTo commitTo) throws Exception;
    }

}
