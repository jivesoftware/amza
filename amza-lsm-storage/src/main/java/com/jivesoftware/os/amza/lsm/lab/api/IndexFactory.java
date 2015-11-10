package com.jivesoftware.os.amza.lsm.lab.api;

import com.jivesoftware.os.amza.lsm.lab.WriteLeapsAndBoundsIndex;
import com.jivesoftware.os.amza.lsm.lab.IndexRangeId;

/**
 *
 * @author jonathan.colt
 */
public interface IndexFactory {

    WriteLeapsAndBoundsIndex createIndex(IndexRangeId id, long worstCaseCount) throws Exception;
}
