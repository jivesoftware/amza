package com.jivesoftware.os.amza.service.stats;

import java.util.concurrent.atomic.LongAdder;

/**
 *
 * @author jonathan.colt
 */
public class NetStats {

    public final LongAdder read = new LongAdder();
    public final LongAdder wrote = new LongAdder();
}
