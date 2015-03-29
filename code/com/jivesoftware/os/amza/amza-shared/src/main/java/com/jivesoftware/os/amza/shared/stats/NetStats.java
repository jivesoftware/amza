package com.jivesoftware.os.amza.shared.stats;

import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class NetStats {

    public final AtomicLong read = new AtomicLong();
    public final AtomicLong wrote = new AtomicLong();
}
