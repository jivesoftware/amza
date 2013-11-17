package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.transport.tcp.replication.messages.FrameableMessage;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public class MessageFramer {

    private final FstMarshaller fstMarshaller;
    private final int headerSize = 4;

    public MessageFramer(FstMarshaller fstMarshaller) {
        this.fstMarshaller = fstMarshaller;
    }

    public void toFrame(FrameableMessage frameable, ByteBuffer writeBuffer) throws IOException {
        //set placeholder size;
        writeBuffer.mark();
        writeBuffer.putInt(0);

        int size = fstMarshaller.serialize(frameable, writeBuffer);
        int position = writeBuffer.position();

        //set actual size value
        writeBuffer.reset();
        writeBuffer.putInt(size);
        writeBuffer.position(position);
    }

    public <F extends FrameableMessage> F fromFrame(ByteBuffer readBuffer, Class<F> clazz) throws Exception {
        int position = readBuffer.position();
        int limit = readBuffer.limit();

        readBuffer.flip();

        if (readBuffer.remaining() > headerSize) {
            int messageLength = readBuffer.getInt();

            if (readBuffer.remaining() >= messageLength) {
                return fstMarshaller.<F>deserialize(readBuffer, clazz);
            }
        }

        //return to it's last state
        readBuffer.limit(limit);
        readBuffer.position(position);

        return null;
    }
}
