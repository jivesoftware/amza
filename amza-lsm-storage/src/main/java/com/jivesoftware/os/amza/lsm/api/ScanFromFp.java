package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public interface ScanFromFp {

    boolean next(long fp, RawPointerStream stream) throws Exception;
}
