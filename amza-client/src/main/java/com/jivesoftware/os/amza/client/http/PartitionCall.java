package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.ring.RingMember;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionCall<C, R, E extends Throwable> {

    PartitionResponse<R> call(RingMember ringMember, C client) throws E;

    public static class PartitionResponse<R> {

        public final R response;
        public final boolean responseComplete;

        public PartitionResponse(R response, boolean responseComplete) {
            this.response = response;
            this.responseComplete = responseComplete;
        }
    }

}
