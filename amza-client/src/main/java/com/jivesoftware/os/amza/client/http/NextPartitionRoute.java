package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.ring.RingHost;

/**
 *
 * @author jonathan.colt
 */
public interface NextPartitionRoute {

    int[] getClients(RingHost[] ringHosts);

    void usedClientAtIndex(int index);

}
