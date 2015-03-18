package com.jivesoftware.os.amza.mavibot;

import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.filer.io.FilerIO;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.directory.mavibot.btree.serializer.AbstractElementSerializer;
import org.apache.directory.mavibot.btree.serializer.BufferHandler;

/**
 *
 * @author jonathan.colt
 */
public class MaviBotRowIndexValueSerializer extends AbstractElementSerializer<RowIndexValue> {

    public MaviBotRowIndexValueSerializer() {
        super(new Comparator<RowIndexValue>() {

            @Override
            public int compare(RowIndexValue o1, RowIndexValue o2) {
                return Long.compare(o1.getTimestampId(), o2.getTimestampId());
            }
        });
    }

    @Override
    public byte[] serialize(RowIndexValue t) {
        ByteBuffer bb = ByteBuffer.allocate(8 + 8 + 1);
        if (t.getValue().length != 8) {
            throw new RuntimeException("This is expected to be a FP");
        }
        bb.put(t.getValue());
        bb.putLong(t.getTimestampId());
        bb.put(t.getTombstoned() ? (byte) 1 : 0);
        return bb.array();
    }

    @Override
    public RowIndexValue deserialize(BufferHandler bh) throws IOException {
        return deserialize(ByteBuffer.wrap(bh.read(8 + 8 + 1)));
    }

    @Override
    public RowIndexValue deserialize(ByteBuffer bb) throws IOException {
        return new RowIndexValue(FilerIO.longBytes(bb.getLong()), bb.getLong(), bb.get() == 1 ? true : false);
    }

    @Override
    public RowIndexValue fromBytes(byte[] bytes) throws IOException {
        return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public RowIndexValue fromBytes(byte[] bytes, int i) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.position(i);
        return deserialize(bb);
    }

}
