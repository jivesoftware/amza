package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 *
 */
public class MessageFramer {

    private final FstMarshaller fstMarshaller;
    private final Map<Integer, Class<? extends Serializable>> payloadRegistry;
    private final int headerSize = 8 + 4 + 1 + 4;

    public MessageFramer(FstMarshaller fstMarshaller, Map<Integer, Class<? extends Serializable>> payloadRegistry) {
        this.fstMarshaller = fstMarshaller;
        this.payloadRegistry = payloadRegistry;
    }

    public void writeFrame(MessageFrame frame, ByteBuffer writeBuffer) throws IOException {
        writeBuffer.putLong(frame.getInteractionId());
        writeBuffer.putInt(frame.getOpCode());
        writeBuffer.put(frame.isLastInSequence() ? (byte) 1 : 0);


        if (frame.getPayload() != null) {
            //set placeholder size;
            writeBuffer.mark();
            writeBuffer.putInt(0);

            int size = fstMarshaller.serialize(frame.getPayload(), writeBuffer);
            int position = writeBuffer.position();

            //set actual size value
            writeBuffer.reset();
            writeBuffer.putInt(size);
            writeBuffer.position(position);
        } else {

            //indicate no payload
            writeBuffer.putInt(0);
        }

        writeBuffer.flip();
    }

    public MessageFrame readFrame(ByteBuffer readBuffer) throws Exception {
        int position = readBuffer.position();
        int limit = readBuffer.limit();

        readBuffer.flip();

        if (readBuffer.remaining() > headerSize) {
            long interactionId = readBuffer.getLong();
            int opCode = readBuffer.getInt();
            boolean lastInSequence = readBuffer.get() == 1;
            int payloadLength = readBuffer.getInt();

            if (payloadLength < 0) {
                throw new IllegalStateException("Encountered invalid message payload length: " + payloadLength);
            }

            Serializable payload = null;
            if (payloadLength > 0) {
                if (readBuffer.remaining() >= payloadLength) {
                    payload = fstMarshaller.deserialize(readBuffer, payloadRegistry.get(opCode));
                } else {
                    //return buffer to its last state
                    readBuffer.limit(limit);
                    readBuffer.position(position);

                    return null;
                }
            }

            return new MessageFrame(interactionId, opCode, lastInSequence, payload);
        }

        return null;
    }
}
