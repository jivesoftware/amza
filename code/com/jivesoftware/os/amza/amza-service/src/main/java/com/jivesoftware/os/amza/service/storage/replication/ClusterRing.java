package com.jivesoftware.os.amza.service.storage.replication;

import com.jivesoftware.os.amza.shared.RingHost;
import java.util.Collection;

public interface ClusterRing {
    Collection<RingHost> ring();
}
