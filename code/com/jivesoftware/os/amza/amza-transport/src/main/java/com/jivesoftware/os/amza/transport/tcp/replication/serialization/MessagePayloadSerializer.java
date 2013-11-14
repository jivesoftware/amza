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
package com.jivesoftware.os.amza.transport.tcp.replication.serialization;

import de.ruedigermoeller.serialization.FSTBasicObjectSerializer;
import de.ruedigermoeller.serialization.FSTClazzInfo;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

/**
 *
 */
public class MessagePayloadSerializer extends FSTBasicObjectSerializer {

    @Override
    public void writeObject(FSTObjectOutput output, Object toSerialize,
        FSTClazzInfo serializationInfo, FSTClazzInfo.FSTFieldInfo referencer, int streamPosition) throws IOException {

        ((MessagePayload) toSerialize).serialize(output);
    }

    @Override
    public MessagePayload instantiate(Class objectClass, FSTObjectInput input, FSTClazzInfo serializationInfo,
        FSTClazzInfo.FSTFieldInfo referencee, int streamPosition) {
        try {
            MessagePayload message = (MessagePayload) objectClass.newInstance();
            message.deserialize(input);
            input.registerObject(message, streamPosition, serializationInfo, referencee);
            return message;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
