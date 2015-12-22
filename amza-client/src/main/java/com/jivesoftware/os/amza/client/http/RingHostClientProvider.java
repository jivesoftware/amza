package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;

/**
 *
 * @author jonathan.colt
 */
public interface RingHostClientProvider<C, E extends Throwable> {

    <R> R call(PartitionName partitionName,
        RingMember leader,
        RingMemberAndHost ringMemberAndHost,
        String family,
        PartitionCall<C, R, E> clientCall) throws Exception;
}
