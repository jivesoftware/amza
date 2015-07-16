package com.jivesoftware.os.amza.lsm;

/**
 *
 * @author jonathan.colt
 */
public interface ImmutableWALPointerBlockHydrator {

    ImmutableWALPointerBlock hydrate(long fp);
}
