package com.jivesoftware.os.amza.client.http;

/**
 *
 * @author jonathan.colt
 */
public class PartitionResponse<R> {
    public final R response;
    public final boolean responseComplete;

    public PartitionResponse(R response, boolean responseComplete) {
        this.response = response;
        this.responseComplete = responseComplete;
    }
}
