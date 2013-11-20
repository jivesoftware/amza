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
package com.jivesoftware.os.amza.storage.binary;

import de.ruedigermoeller.serialization.FSTBasicObjectSerializer;
import de.ruedigermoeller.serialization.FSTClazzInfo;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

public class FSTBinaryRowMarshaller extends FSTBasicObjectSerializer {

    @Override
    public void writeObject(FSTObjectOutput out,
            Object toWrite,
            FSTClazzInfo fSTClazzInfo,
            FSTClazzInfo.FSTFieldInfo fSTFieldInfo,
            int streamPositioin) throws IOException {
        BinaryRow br = (BinaryRow) toWrite;
        out.writeFLong(br.transaction);
        out.writeFInt(br.key.length);
        out.write(br.key);
        out.writeFLong(br.timestamp);
        out.writeFByte(br.tombstone ? 1 : 0);
        out.writeFInt(br.value.length);
        out.write(br.value);
    }

    @Override
    public Object instantiate(Class objectClass,
            FSTObjectInput in,
            FSTClazzInfo serializationInfo,
            FSTClazzInfo.FSTFieldInfo referencee,
            int streamPositioin) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        try {
            long transaction = in.readFLong();
            int keyLength = in.readFInt();
            byte[] key = new byte[keyLength];
            in.readFully(key);
            long timestamp = in.readFLong();
            boolean tombstone = in.readFByte() == 1;
            int valueLength = in.readFInt();
            byte[] value = new byte[valueLength];
            in.readFully(value);
            Object object = new BinaryRow(transaction, key, timestamp, tombstone, value);
            in.registerObject(object, streamPositioin, serializationInfo, referencee);
            return object;
        } catch (Exception x) {
            throw new IOException(x);
        }
    }

}