package com.jivesoftware.os.amza.shared.take;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;

/**
 *
 * @author jonathan.colt
 */
public interface IsNominated {

    boolean isNominated(RingMember ringMember, VersionedPartitionName versionedPartitionName) throws Exception;
}
