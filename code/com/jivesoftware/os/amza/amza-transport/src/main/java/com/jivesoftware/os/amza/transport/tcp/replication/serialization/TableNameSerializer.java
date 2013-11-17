package com.jivesoftware.os.amza.transport.tcp.replication.serialization;

import de.ruedigermoeller.serialization.FSTBasicObjectSerializer;
import de.ruedigermoeller.serialization.FSTClazzInfo;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

/**
 *
 */
public class TableNameSerializer extends FSTBasicObjectSerializer {

    @Override
    public void writeObject(FSTObjectOutput output, Object toSerialize, FSTClazzInfo serializationInfo,
        FSTClazzInfo.FSTFieldInfo referencer, int streamPosition) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object instantiate(Class objectClass, FSTObjectInput in, FSTClazzInfo serializationInfo, FSTClazzInfo.FSTFieldInfo referencee,
        int streamPosition) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        return super.instantiate(objectClass, in, serializationInfo, referencee, streamPosition);
    }
}
