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
