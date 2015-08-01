package com.jivesoftware.os.amza.shared.wal;

/**
 * @author jonathan.colt
 */
public interface FpStream {

    boolean stream(long fp) throws Exception;

}
