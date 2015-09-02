package com.jivesoftware.os.amza.aquarium;

import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;

/**
 * @author jonathan.colt
 */
public class Aquarium {

    private final OrderIdProvider versionProvider;
    private final ReadWaterlineTx waterlineTx;
    private final Liveliness liveliness;
    private final TransitionQuorum transitionCurrent;
    private final TransitionQuorum transitionDesired;
    private final Member member;

    public Aquarium(OrderIdProvider versionProvider,
        ReadWaterlineTx waterlineTx,
        Liveliness liveliness, TransitionQuorum transitionCurrent,
        TransitionQuorum transitionDesired,
        Member member) {
        this.versionProvider = versionProvider;
        this.waterlineTx = waterlineTx;
        this.liveliness = liveliness;
        this.transitionCurrent = transitionCurrent;
        this.transitionDesired = transitionDesired;
        this.member = member;
    }

    public void feedTheFish() throws Exception {
        liveliness.blowBubbles();
        liveliness.acknowledgeOther();
    }

    public void tapTheGlass() throws Exception {
        waterlineTx.tx(member, (current, desired) -> {
            current.acknowledgeOther();
            desired.acknowledgeOther();

            Waterline currentWaterline = current.get();
            if (currentWaterline == null) {
                currentWaterline = new Waterline(member, State.bootstrap, versionProvider.nextId(), -1L, true, Long.MAX_VALUE);
            }
            Waterline desiredWaterline = desired.get();
            currentWaterline.getState().transistor.advance(currentWaterline,
                current,
                transitionCurrent,
                desiredWaterline,
                desired,
                transitionDesired);

            return true;
        });
    }

    public State getState(Member member) throws Exception {
        State[] state = new State[1];
        waterlineTx.tx(member, (readCurrent, readDesired) -> {
            Waterline current = readCurrent.get();
            if (current == null) {
                state[0] = State.bootstrap;
            } else {
                state[0] = current.getState();
            }
            return true;
        });
        return state[0];
    }

    public void expunge(Member member) throws Exception {
        waterlineTx.tx(member, (readCurrent, readDesired) -> {
            transitionDesired.transition(readDesired.get(), versionProvider.nextId(), State.expunged);
            return true;
        });
        tapTheGlass();
    }
}
