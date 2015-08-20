package com.jivesoftware.os.amza.client.http;

import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public interface Merger<R, A> {

    R merge(List<RingHostAnswer<A>> answers) throws Exception;

}
