package com.jivesoftware.os.amza.mavibot;

import com.jivesoftware.os.amza.shared.RowIndexKey;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.directory.mavibot.btree.serializer.AbstractElementSerializer;
import org.apache.directory.mavibot.btree.serializer.BufferHandler;

/**
 *
 * @author jonathan.colt
 */
public class MaviBotRowIndexKeySerializer extends AbstractElementSerializer<RowIndexKey> {

    public MaviBotRowIndexKeySerializer() {
        super(new Comparator<RowIndexKey>() {

            @Override
            public int compare(RowIndexKey o1, RowIndexKey o2) {
                return o1.compareTo(o2);
            }
        });
    }

    @Override
    public byte[] serialize(RowIndexKey t) {
        byte[] key = t.getKey();
        ByteBuffer bb = ByteBuffer.allocate(4 + key.length);
        bb.putInt(key.length);
        bb.put(key);
        return bb.array();
    }

    @Override
    public RowIndexKey deserialize(BufferHandler bh) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(bh.getBuffer());
        int l = bb.getInt();
        byte[] key = new byte[l];
        bb.get(key);
        return new RowIndexKey(key);
    }

    @Override
    public RowIndexKey deserialize(ByteBuffer bb) throws IOException {
        int l = bb.getInt();
        byte[] key = new byte[l];
        bb.get(key);
        return new RowIndexKey(key);
    }

    @Override
    public RowIndexKey fromBytes(byte[] bytes) throws IOException {
        return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public RowIndexKey fromBytes(byte[] bytes, int i) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.position(i);
        return deserialize(bb);
    }

}
