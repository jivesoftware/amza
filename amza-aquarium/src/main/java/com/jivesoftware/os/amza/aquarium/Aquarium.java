package com.jivesoftware.os.amza.aquarium;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;

/**
 *
 * @author jonathan.colt
 */
public class Aquarium {

    private final OrderIdProvider versionProvider;
    private final ReadWaterlineTx waterlineTx;
    private final TransitionQuorum transitionCurrent;
    private final TransitionQuorum transitionDesired;
    private final Member member;

    public Aquarium(OrderIdProvider versionProvider,
        ReadWaterlineTx waterlineTx,
        TransitionQuorum transitionCurrent,
        TransitionQuorum transitionDesired,
        Member member) {
        this.versionProvider = versionProvider;
        this.waterlineTx = waterlineTx;
        this.transitionCurrent = transitionCurrent;
        this.transitionDesired = transitionDesired;
        this.member = member;
    }

    public void tapTheGlass(int ringSize) throws Exception {

        waterlineTx.tx(ringSize, member, (current, desired) -> {
            current.acknowledgeOther();
            desired.acknowledgeOther();

            ReadWaterline.Waterline currentWaterline = current.get();
            if (currentWaterline == null) {
                currentWaterline = new ReadWaterline.Waterline(member, State.bootstrap, versionProvider.nextId(), true);
            }
            ReadWaterline.Waterline desiredWaterline = desired.get();
            currentWaterline.getState().transistor.advance(currentWaterline,
                current,
                transitionCurrent,
                desiredWaterline,
                desired,
                transitionDesired);

            return true;
        });

    }
}
