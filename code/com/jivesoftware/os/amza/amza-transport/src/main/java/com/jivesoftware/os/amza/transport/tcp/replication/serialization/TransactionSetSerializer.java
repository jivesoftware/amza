package com.jivesoftware.os.amza.transport.tcp.replication.serialization;

import de.ruedigermoeller.serialization.FSTBasicObjectSerializer;
import de.ruedigermoeller.serialization.FSTClazzInfo;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

/**
 *
 */
public class TransactionSetSerializer extends FSTBasicObjectSerializer {

    @Override
    public void writeObject(FSTObjectOutput fstoo, Object o, FSTClazzInfo fstci, FSTClazzInfo.FSTFieldInfo fstfi, int i) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
