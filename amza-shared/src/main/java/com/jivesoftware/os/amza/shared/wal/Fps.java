package com.jivesoftware.os.amza.shared.wal;

/**
 * @author jonathan.colt
 */
public interface Fps {

    boolean consume(FpStream fpStream) throws Exception;

}
