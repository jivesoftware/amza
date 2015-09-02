package com.jivesoftware.os.amza.aquarium;

/**
 *
 * @author jonathan.colt
 */
public interface ReadWaterlineTx {

    void tx(Member member, Tx tx) throws Exception;

    interface Tx {

        boolean tx(ReadWaterline readCurrent, ReadWaterline readDesired) throws Exception;
    }

}
