package com.jivesoftware.os.amza.lsm.api;

import com.jivesoftware.os.amza.lsm.DiskBackedLeapPointerIndex;

/**
 *
 * @author jonathan.colt
 */
public interface PointerIndexFactory {

    DiskBackedLeapPointerIndex createPointerIndex() throws Exception;
}
