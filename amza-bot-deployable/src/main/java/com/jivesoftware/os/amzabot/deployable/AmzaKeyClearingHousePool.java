package com.jivesoftware.os.amzabot.deployable;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AmzaKeyClearingHousePool {

    private Set<AmzaKeyClearingHouse> amzaKeyClearingHouseSet =
        Collections.newSetFromMap(new ConcurrentHashMap<AmzaKeyClearingHouse, Boolean>());

    public AmzaKeyClearingHouse genAmzaKeyClearingHouse() {
        AmzaKeyClearingHouse res = new AmzaKeyClearingHouse();
        amzaKeyClearingHouseSet.add(res);
        return res;
    }

    public AmzaKeyClearingHouse genAmzaKeyClearingHouse(long capacity) {
        AmzaKeyClearingHouse res = new AmzaKeyClearingHouse(capacity);
        amzaKeyClearingHouseSet.add(res);
        return res;
    }

    public void removeAmzaKeyClearingHouse(AmzaKeyClearingHouse amzaKeyClearingHouse) {
        amzaKeyClearingHouseSet.remove(amzaKeyClearingHouse);
    }

    public ConcurrentMap<String, Entry<String, String>> getAllQuarantinedEntries() {
        ConcurrentMap<String, Entry<String, String>> res = Maps.newConcurrentMap();

        for (AmzaKeyClearingHouse amzaKeyClearingHouse : amzaKeyClearingHouseSet) {
            res.putAll(amzaKeyClearingHouse.getQuarantinedKeyMap());
        }

        return res;
    }

}
