package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.storage.FstMarshaller;
import de.ruedigermoeller.serialization.FSTConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FSTBinaryRowMarshallerTest {

    @Test
    public void testMarshalling() throws Exception {
        FstMarshaller fstMarshaller = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());
        fstMarshaller.registerSerializer(BinaryRow.class, new FSTBinaryRowMarshaller());

        byte[] helloAsBytes = fstMarshaller.serialize("Hello");
        System.out.println(fstMarshaller.deserialize(helloAsBytes, String.class));

        BinaryRow binaryRow = new BinaryRow(1, new byte[]{1}, 2, true, new byte[]{3});
        byte[] serialize = fstMarshaller.serialize(binaryRow);
        BinaryRow binaryRow2 = fstMarshaller.deserialize(serialize, BinaryRow.class);

        Assert.assertEquals(binaryRow, binaryRow2);

    }

}
