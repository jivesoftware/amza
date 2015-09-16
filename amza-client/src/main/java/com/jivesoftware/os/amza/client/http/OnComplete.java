package com.jivesoftware.os.amza.client.http;

/**
 * @author jonathan.colt
 */
public interface OnComplete<A> {

    void complete(Iterable<A> answers) throws Exception;

}
