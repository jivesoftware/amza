package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.shared.wal.WALKeyPointers;

/**
 *
 * @author jonathan.colt
 */
public interface AppendableWalIndex {

    void append(WALKeyPointers pointerStream) throws Exception;

}
