package com.jivesoftware.os.amza.service.take;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;

/**
 *
 * @author jonathan.colt
 */
public interface CheckState {

    boolean isNominated(RingMember ringMember, VersionedPartitionName versionedPartitionName) throws Exception;

    boolean isOnline(RingMember ringMember, VersionedPartitionName versionedPartitionName) throws Exception;
}
