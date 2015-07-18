package com.jivesoftware.os.amza.lsm;

/**
 *
 * @author jonathan.colt
 */
public interface LsmWalIndexFactory {

    LsmWalIndex createWALIndex() throws Exception;
}
