package com.jivesoftware.os.amza.lsm.lab.api;

import com.jivesoftware.os.amza.lsm.lab.LeapsAndBoundsIndex;

/**
 *
 * @author jonathan.colt
 */
public interface CommitIndex {

    LeapsAndBoundsIndex commit(LeapsAndBoundsIndex index) throws Exception;

}
