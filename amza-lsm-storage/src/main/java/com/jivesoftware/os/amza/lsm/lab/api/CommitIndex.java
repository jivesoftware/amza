package com.jivesoftware.os.amza.lsm.lab.api;

import com.jivesoftware.os.amza.lsm.lab.IndexRangeId;
import com.jivesoftware.os.amza.lsm.lab.LeapsAndBoundsIndex;
import com.jivesoftware.os.amza.lsm.lab.WriteLeapsAndBoundsIndex;

/**
 *
 * @author jonathan.colt
 */
public interface CommitIndex {

    LeapsAndBoundsIndex commit(IndexRangeId id, WriteLeapsAndBoundsIndex index) throws Exception;

}
