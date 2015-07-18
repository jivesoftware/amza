package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;

/**
 *
 * @author jonathan.colt
 */
public interface FeedNext {

    boolean feedNext(WALKeyPointerStream stream) throws Exception;
}
