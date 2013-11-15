/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import de.ruedigermoeller.serialization.FSTBasicObjectSerializer;
import de.ruedigermoeller.serialization.FSTClazzInfo;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

/**
 *
 */
public class FrameableSerializer extends FSTBasicObjectSerializer {

    @Override
    public void writeObject(FSTObjectOutput output, Object toSerialize,
        FSTClazzInfo classInfo, FSTClazzInfo.FSTFieldInfo fieldInfo, int mysteryInt) throws IOException {

        ((FrameableMessage) toSerialize).serialize(output);
    }
}
