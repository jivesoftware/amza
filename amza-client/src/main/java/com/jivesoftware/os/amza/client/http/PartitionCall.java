package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.ring.RingMember;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionCall<C, R, E extends Throwable> {

    PartitionResponse<R> call(RingMember leader, RingMember ringMember, C client) throws E;

}
