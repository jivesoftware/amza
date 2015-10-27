package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;

/**
 *
 * @author jonathan.colt
 */
public class RingMemberAndHostAnswer<A> {

    private final RingMemberAndHost ringMemberAndHost;
    private final A answer;

    public RingMemberAndHostAnswer(RingMemberAndHost ringMemberAndHost, A answer) {
        this.ringMemberAndHost = ringMemberAndHost;
        this.answer = answer;
    }

    public RingMemberAndHost getRingMemberAndHost() {
        return ringMemberAndHost;
    }

    public A getAnswer() {
        return answer;
    }

    @Override
    public String toString() {
        return "RingMemberAndHostAnswer{" + "ringMemberAndHost=" + ringMemberAndHost + ", answer=" + answer + '}';
    }
}
