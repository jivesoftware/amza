package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.nio.ByteBuffer;

/**
 *
 */
public class MessageFramer {

    private final byte eos = -1;

    public ByteBuffer buildFrame(byte[] payload, boolean lastInSequence) {
        ByteBuffer frame;

        if (lastInSequence) {
            frame = ByteBuffer.allocate(4 + payload.length + 1);
            frame.putInt(payload.length);
            frame.put(payload);
            frame.put(eos);
        } else {
            frame = ByteBuffer.allocate(4 + payload.length);
            frame.putInt(payload.length);
            frame.put(payload);
        }

        return frame;
    }

    public Frame readFrame(ByteBuffer buffer) {
        return null; //TODO I'll need to do better than this
    }

    public static class Frame {

        private final byte[] message;
        private final boolean lastInSequence;

        public Frame(byte[] message, boolean lastInSequence) {
            this.message = message;
            this.lastInSequence = lastInSequence;
        }

        public byte[] getMessage() {
            return message;
        }

        public boolean isLastInSequence() {
            return lastInSequence;
        }
    }
}
