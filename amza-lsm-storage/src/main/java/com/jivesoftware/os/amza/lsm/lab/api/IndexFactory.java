package com.jivesoftware.os.amza.lsm.lab.api;

import com.jivesoftware.os.amza.lsm.lab.IndexRangeId;
import com.jivesoftware.os.amza.lsm.lab.WriteLeapsAndBoundsIndex;

/**
 *
 * @author jonathan.colt
 */
public interface IndexFactory {

    WriteLeapsAndBoundsIndex createIndex(IndexRangeId id, long worstCaseCount) throws Exception;
}
