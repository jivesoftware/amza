package com.jivesoftware.os.amza.lsm.api;

import com.jivesoftware.os.amza.lsm.DiskBackedPointerIndex;

/**
 *
 * @author jonathan.colt
 */
public interface PointerIndexFactory {

    DiskBackedPointerIndex createPointerIndex() throws Exception;
}
