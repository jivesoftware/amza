package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.nio.ByteBuffer;

/**
 *
 */
public class MessageFramer {

    public void buildFrame(byte[] payload, boolean lastInSequence, ByteBuffer frameBuffer) {
        frameBuffer = ByteBuffer.allocate(5 + payload.length);
        frameBuffer.put(lastInSequence ? (byte) 1 : 0);
        frameBuffer.putInt(payload.length);
        frameBuffer.put(payload);
    }

    public Frame readFrame(ByteBuffer buffer) {
        byte eos = buffer.get();
        if (eos != 1 && eos != 0) {
            throw new IllegalStateException("Invalid eos signifie read from stream");
        }

        int length = buffer.getInt();
        byte[] message = new byte[length];
        buffer.get(message, buffer.position(), length);

        return new Frame(message, eos == 1);
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
