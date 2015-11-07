package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public interface RawAppendablePointerIndex {

    boolean append(RawPointers pointers) throws Exception;

    void commit() throws Exception;
}
