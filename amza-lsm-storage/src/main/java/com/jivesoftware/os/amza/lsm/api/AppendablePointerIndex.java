package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public interface AppendablePointerIndex {

    boolean append(Pointers pointers) throws Exception;

    void commit() throws Exception;
}
