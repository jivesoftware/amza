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
