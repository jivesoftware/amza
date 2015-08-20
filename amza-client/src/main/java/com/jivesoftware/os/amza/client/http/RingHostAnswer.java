package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.ring.RingHost;

/**
 *
 * @author jonathan.colt
 */
public class RingHostAnswer<A> {
    private final RingHost ringHost;
    private final A answer;

    public RingHostAnswer(RingHost ringHost, A answer) {
        this.ringHost = ringHost;
        this.answer = answer;
    }

    public RingHost getRingHost() {
        return ringHost;
    }

    public A getAnswer() {
        return answer;
    }

    @Override
    public String toString() {
        return "RingHostAnswer{" + "ringHost=" + ringHost + ", answer=" + answer + '}';
    }

}
