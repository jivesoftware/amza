package com.jivesoftware.os.amza.storage.binary;

import de.ruedigermoeller.serialization.FSTBasicObjectSerializer;
import de.ruedigermoeller.serialization.FSTClazzInfo;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

public class FSTBinaryRowSerializer extends FSTBasicObjectSerializer {

    @Override
    public void writeObject(FSTObjectOutput out,
            Object toWrite,
            FSTClazzInfo fSTClazzInfo,
            FSTClazzInfo.FSTFieldInfo fSTFieldInfo,
            int streamPositioin) throws IOException {
        out.writeFLong(((BinaryRow) toWrite).transaction);
        out.writeClass(((BinaryRow) toWrite).key);
        out.writeFLong(((BinaryRow) toWrite).timestamp);
        out.writeFByte(((BinaryRow) toWrite).tombstone ? 1 : 0);
        out.writeClass(((BinaryRow) toWrite).value);
    }

    @Override
    public void readObject(FSTObjectInput in,
            Object toRead,
            FSTClazzInfo clzInfo,
            FSTClazzInfo.FSTFieldInfo referencedBy) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
    }

    @Override
    public Object instantiate(Class objectClass,
            FSTObjectInput in,
            FSTClazzInfo serializationInfo,
            FSTClazzInfo.FSTFieldInfo referencee,
            int streamPositioin) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        try {
            Object object = new BinaryRow(in.readFLong(),
                    (byte[]) in.readObject(byte[].class),
                    in.readFLong(),
                    in.readFByte() == 1,
                    (byte[]) in.readObject(byte[].class));
            in.registerObject(object, streamPositioin, serializationInfo, referencee);
            return object;
        } catch (Exception x) {
            throw new IOException(x);
        }
    }

}