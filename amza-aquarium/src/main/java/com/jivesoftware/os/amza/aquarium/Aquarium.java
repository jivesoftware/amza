package com.jivesoftware.os.amza.aquarium;

import com.jivesoftware.os.amza.aquarium.ReadWaterlineTx.Tx;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;

/**
 * @author jonathan.colt
 */
public class Aquarium {

    private final OrderIdProvider versionProvider;
    private final CurrentTimeMillis currentTimeMillis;
    private final ReadWaterlineTx waterlineTx;
    private final TransitionQuorum transitionCurrent;
    private final TransitionQuorum transitionDesired;
    private final Member member;

    public Aquarium(OrderIdProvider versionProvider,
        CurrentTimeMillis currentTimeMillis,
        ReadWaterlineTx waterlineTx,
        TransitionQuorum transitionCurrent,
        TransitionQuorum transitionDesired,
        Member member) {
        this.versionProvider = versionProvider;
        this.currentTimeMillis = currentTimeMillis;
        this.waterlineTx = waterlineTx;
        this.transitionCurrent = transitionCurrent;
        this.transitionDesired = transitionDesired;
        this.member = member;
    }

    public void inspectState(Member member, Tx tx) throws Exception {
        waterlineTx.tx(member, tx);
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
            currentWaterline.getState().transistor.advance(currentTimeMillis, currentWaterline,
                current,
                transitionCurrent,
                desiredWaterline,
                desired,
                transitionDesired);

            return true;
        });
    }

    /**
     * @return null, leader or follower
     */
    public State livelyEndState() throws Exception { // TODO consider timeout and wait notify bla...
        State[] state = {null};
        waterlineTx.tx(member, (current, desired) -> {

            Waterline currentWaterline = current.get();
            Waterline desiredWaterline = desired.get();

            if (currentWaterline != null
                && currentWaterline.isAlive(currentTimeMillis.get())
                && currentWaterline.isAtQuorum()
                && State.checkEquals(currentTimeMillis, currentWaterline, desiredWaterline)) {

                if (currentWaterline.getState() == State.leader) {
                    state[0] = State.leader;
                }
                if (currentWaterline.getState() == State.follower) {
                    state[0] = State.follower;
                }
            }
            return true;
        });
        return state[0];
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
