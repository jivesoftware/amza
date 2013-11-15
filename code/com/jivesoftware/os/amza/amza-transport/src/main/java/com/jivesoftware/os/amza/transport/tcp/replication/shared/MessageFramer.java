package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public class MessageFramer {

    private final FstMarshaller fstMarshaller;
    private final int headerSize = 1 + 1 + 4;

    public MessageFramer(FstMarshaller fstMarshaller) {
        this.fstMarshaller = fstMarshaller;
    }

    public void toFrame(FrameableMessage frameable, ByteBuffer buff) throws IOException {
        //set placeholder size;
        buff.mark();
        buff.putInt(0);
        int size = fstMarshaller.serialize(frameable, buff);

        //set actual size value
        buff.reset();
        buff.putInt(size);

        //prepare for socket to read buffer during send
        buff.rewind();
    }

    public <F extends FrameableMessage> F fromFrame(ByteBuffer readBuffer, int read, Class<F> clazz) throws Exception {
        readBuffer.mark();
        readBuffer.flip();

        if (readBuffer.remaining() > headerSize) {
            int messageLength = readBuffer.getInt();

            if (readBuffer.remaining() >= messageLength) {
                return fstMarshaller.<F>deserialize(readBuffer, clazz);
            }

        }

        //return to it's last position so next read will append bytes
        readBuffer.reset();
        return null;
    }
}
