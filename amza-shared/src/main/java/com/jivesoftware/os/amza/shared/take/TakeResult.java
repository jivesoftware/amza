package com.jivesoftware.os.amza.shared.take;

import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;

/**
 *
 * @author jonathan.colt
 */
public class TakeResult {

    public final RingMember tookFrom;
    public final long lastTxId;
    public final WALHighwater tookToEnd;

    public TakeResult(RingMember tookFrom, long lastTxId, WALHighwater tookToEnd) {
        this.tookFrom = tookFrom;
        this.lastTxId = lastTxId;
        this.tookToEnd = tookToEnd;
    }

}
