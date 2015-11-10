package com.jivesoftware.os.amza.lsm.lab.api;

import com.jivesoftware.os.amza.lsm.lab.LeapsAndBoundsIndex;

/**
 *
 * @author jonathan.colt
 */
public interface IndexFactory {

    LeapsAndBoundsIndex createIndex(long worstCaseCount) throws Exception;
}
