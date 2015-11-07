package com.jivesoftware.os.amza.lsm.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface MergeRawEntry {

    byte[] merge(byte[] current, byte[] adding);
}
