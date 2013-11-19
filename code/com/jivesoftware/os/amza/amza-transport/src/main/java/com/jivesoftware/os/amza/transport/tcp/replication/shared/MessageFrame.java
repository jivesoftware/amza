package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.io.Serializable;
import java.util.Objects;

/**
 *
 */
public class MessageFrame {

    private final long interactionId;
    private final int opCode;
    private final boolean lastInSequence;
    private final Serializable payload;

    public MessageFrame(long interactionId, int opCode, boolean lastInSequence, Serializable payload) {
        this.interactionId = interactionId;
        this.opCode = opCode;
        this.lastInSequence = lastInSequence;
        this.payload = payload;
    }

    public MessageFrame(long interactionId, int opCode, boolean lastInSequence) {
        this(interactionId, opCode, lastInSequence, null);
    }

    public long getInteractionId() {
        return interactionId;
    }

    public int getOpCode() {
        return opCode;
    }

    public boolean isLastInSequence() {
        return lastInSequence;
    }

    public <P extends Serializable> P getPayload() {
        return (P) payload;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 29 * hash + (int) (this.interactionId ^ (this.interactionId >>> 32));
        hash = 29 * hash + this.opCode;
        hash = 29 * hash + (this.lastInSequence ? 1 : 0);
        hash = 29 * hash + Objects.hashCode(this.payload);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MessageFrame other = (MessageFrame) obj;
        if (this.interactionId != other.interactionId) {
            return false;
        }
        if (this.opCode != other.opCode) {
            return false;
        }
        if (this.lastInSequence != other.lastInSequence) {
            return false;
        }
        if (!Objects.equals(this.payload, other.payload)) {
            return false;
        }
        return true;
    }
}
