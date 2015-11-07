package com.jivesoftware.os.amza.lsm.api;

/**
 *
 * @author jonathan.colt
 */
public interface MergeRawEntry {

    byte[] merge(byte[] current, byte[] adding);
}
