package com.jivesoftware.os.amzabot.deployable;

import com.google.common.collect.Maps;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

class AmzaKeyClearingHouse {

    private Random rand = new Random();

    // key, value
    private ConcurrentMap<String, String> keyMap = Maps.newConcurrentMap();

    ConcurrentMap<String, String> getKeyMap() {
        return keyMap;
    }

    void clearKeyMap() {
        keyMap.clear();
    }

    // key, <expected value, actual value>
    private ConcurrentMap<String, Entry<String, String>> quarantinedKeyMap = Maps.newConcurrentMap();

    ConcurrentMap<String, Entry<String, String>> getQuarantinedKeyMap() {
        return quarantinedKeyMap;
    }

    void clearQuarantinedKeyMap() {
        quarantinedKeyMap.clear();
    }

    public String set(String key, String value) {
        return keyMap.put(key, value);
    }

    public String get(String key) {
        return keyMap.get(key);
    }

    void delete(String key) {
        keyMap.remove(key);
    }

    Entry<String, String> getRandomKey() {
        if (keyMap.isEmpty()) {
            return null;
        }

        List<Entry<String, String>> array = new ArrayList<>(keyMap.entrySet());
        int randIdx = rand.nextInt(keyMap.size());

        return array.get(randIdx);
    }

    void quarantineKey(Entry<String, String> entry, String value) {
        quarantinedKeyMap.put(entry.getKey(),
            new AbstractMap.SimpleEntry<>(entry.getValue(), value));
        keyMap.remove(entry.getKey());
    }
}
