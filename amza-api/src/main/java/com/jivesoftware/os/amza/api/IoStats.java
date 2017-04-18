package com.jivesoftware.os.amza.api;

import java.util.concurrent.atomic.LongAdder;

/**
 *
 * @author jonathan.colt
 */
public class IoStats {

    public final LongAdder read = new LongAdder();
    public final LongAdder wrote = new LongAdder();

}
