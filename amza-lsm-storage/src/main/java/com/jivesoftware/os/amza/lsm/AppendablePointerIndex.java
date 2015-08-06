package com.jivesoftware.os.amza.lsm;

/**
 *
 * @author jonathan.colt
 */
public interface AppendablePointerIndex {

    void append(Pointers pointers) throws Exception;

}
