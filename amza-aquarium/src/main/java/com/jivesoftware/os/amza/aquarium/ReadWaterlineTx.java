package com.jivesoftware.os.amza.aquarium;

/**
 *
 * @author jonathan.colt
 */
public interface ReadWaterlineTx {

    void tx(int ringSize, Member member, Tx tx) throws Exception;

    interface Tx {

        boolean tx(ReadWaterline current, ReadWaterline desired) throws Exception;
    }

}
