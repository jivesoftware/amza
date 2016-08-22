package com.jivesoftware.os.amzabot.deployable;

import com.google.common.collect.Maps;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

public class AmzaKeyClearingHouse {

    private Random rand = new Random();

    // key, value
    private ConcurrentMap<String, String> keyMap = Maps.newConcurrentMap();

    public ConcurrentMap<String, String> getKeyMap() {
        return keyMap;
    }

    // key, <expected value, actual value>
    private ConcurrentMap<String, Entry<String, String>> quarantinedKeyMap = Maps.newConcurrentMap();

    public ConcurrentMap<String, Entry<String, String>> getQuarantinedKeyMap() {
        return quarantinedKeyMap;
    }

    public void clearQuarantinedKeys() {
        quarantinedKeyMap.clear();
    }

    public String set(String key, String value) {
        return keyMap.put(key, value);
    }

    public String get(String key) {
        return keyMap.get(key);
    }

    public void delete(String key) {
        keyMap.remove(key);
    }

    public Entry<String, String> getRandomKey() {
        if (keyMap.isEmpty()) {
            return null;
        }

        List<Entry<String, String>> array = new ArrayList<>(keyMap.entrySet());
        int randIdx = rand.nextInt(keyMap.size());

        return array.get(randIdx);
    }

    public void quarantineKey(Entry<String, String> entry, String value) {
        quarantinedKeyMap.put(entry.getKey(),
            new AbstractMap.SimpleEntry<>(entry.getValue(), value));
        keyMap.remove(entry.getKey());
    }
}
