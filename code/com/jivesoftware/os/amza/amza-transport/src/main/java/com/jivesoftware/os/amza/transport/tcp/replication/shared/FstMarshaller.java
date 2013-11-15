package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import de.ruedigermoeller.serialization.FSTBasicObjectSerializer;
import de.ruedigermoeller.serialization.FSTConfiguration;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 *
 */
public class FstMarshaller {

    private final FSTConfiguration fstConfig;

    public FstMarshaller(FSTConfiguration fstConfig) {
        this.fstConfig = fstConfig;
    }

    public void registerSerializer(Class clazz, FSTBasicObjectSerializer serializer) {
        fstConfig.registerSerializer(clazz, serializer, false);
    }

    public <V extends Serializable> int serialize(V toSerialize, ByteBuffer buffer) throws IOException {
        int start = buffer.position();

        ByteBufferOutputStream bbos = new ByteBufferOutputStream(buffer);
        try (FSTObjectOutput out = fstConfig.getObjectOutput(bbos)) {
            out.writeObject(out, toSerialize.getClass());
            out.flush();
        }

        return buffer.position() - start;
    }

    public <V> V deserialize(ByteBuffer readBuffer, Class<V> clazz) throws Exception {
        ByteBufferInputStream bbis = new ByteBufferInputStream(readBuffer);
        try (FSTObjectInput in = fstConfig.getObjectInput(bbis)) {
            return (V) in.readObject(clazz);
        }
    }
}
