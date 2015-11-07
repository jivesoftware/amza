package com.jivesoftware.os.amza.lsm.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface ScanFromFp {

    boolean next(long fp, RawEntryStream stream) throws Exception;
}
