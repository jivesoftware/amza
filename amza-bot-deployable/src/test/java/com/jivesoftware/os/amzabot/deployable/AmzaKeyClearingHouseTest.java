package com.jivesoftware.os.amzabot.deployable;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Map.Entry;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AmzaKeyClearingHouseTest {

    @Test
    public void testVerifyGoodKeyMap() throws Exception {
        Map<String, String> partitionMap = Maps.newHashMap();

        for (int i = 0; i < 10; i++) {
            partitionMap.put("key:" + i, "value:" + i);
        }

        AmzaKeyClearingHouse amzaKeyClearingHouse = new AmzaKeyClearingHouse();
        for (Entry<String, String> entry : partitionMap.entrySet()) {
            amzaKeyClearingHouse.set(entry.getKey(), entry.getValue());
        }

        Assert.assertTrue(amzaKeyClearingHouse.verifyKeyMap(partitionMap));
    }

    @Test
    public void testVerifySwappedKeyMap() throws Exception {
        Map<String, String> partitionMap = Maps.newHashMap();
        partitionMap.put("foo", "bar");
        partitionMap.put("ham", "eggs");

        AmzaKeyClearingHouse amzaKeyClearingHouse = new AmzaKeyClearingHouse();
        for (Entry<String, String> entry : partitionMap.entrySet()) {
            amzaKeyClearingHouse.set(entry.getKey(), entry.getValue() + "-foo");
        }

        Assert.assertFalse(amzaKeyClearingHouse.verifyKeyMap(partitionMap));
    }

}
