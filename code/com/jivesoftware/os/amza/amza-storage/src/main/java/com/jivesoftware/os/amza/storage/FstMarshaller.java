/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.storage;

import com.jivesoftware.os.amza.shared.Marshaller;
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
public class FstMarshaller implements Marshaller {

    private final FSTConfiguration fstConfig;

    public FstMarshaller(FSTConfiguration fstConfig) {
        this.fstConfig = fstConfig;
    }

    public void registerSerializer(Class clazz, FSTBasicObjectSerializer serializer) {
        fstConfig.registerSerializer(clazz, serializer, false);
    }

    @Override
    public <V> byte[] serialize(V value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (FSTObjectOutput out = new FSTObjectOutput(baos)) {
            out.writeObject(value, value.getClass());
        }
        return baos.toByteArray();
    }

    @Override
    public <V> V deserialize(byte[] bytes, Class<V> clazz) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try (FSTObjectInput in = new FSTObjectInput(bais)) {
            return (V) in.readObject(clazz);
        }
    }

}
