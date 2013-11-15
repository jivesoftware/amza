/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import de.ruedigermoeller.serialization.FSTBasicObjectSerializer;
import de.ruedigermoeller.serialization.FSTConfiguration;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

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

    public <V> byte[] serialize(V changeSet) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (FSTObjectOutput out = fstConfig.getObjectOutput(baos)) {
            out.writeObject(out, changeSet.getClass());
        }

        return baos.toByteArray();
    }

    public <V> V deserialize(byte[] bytes, Class<V> clazz) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try (FSTObjectInput in = fstConfig.getObjectInput(bais)) {
            return (V) in.readObject(clazz);
        }
    }

}
