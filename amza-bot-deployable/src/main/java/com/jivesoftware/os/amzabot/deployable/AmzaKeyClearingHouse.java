package com.jivesoftware.os.amzabot.deployable;

import com.google.common.collect.Maps;
import com.jivesoftware.os.mlogger.core.AtomicCounter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.RandomStringUtils;

public class AmzaKeyClearingHouse {

    private final Random RANDOM = new Random();

    private final AtomicBoolean honorCapacity = new AtomicBoolean(false);
    private final AtomicCounter currentCapacity = new AtomicCounter();

    // key, value
    private ConcurrentMap<String, String> keyMap = Maps.newConcurrentMap();

    public AmzaKeyClearingHouse() {
    }

    public AmzaKeyClearingHouse(long capacity) {
        honorCapacity.set(true);
        currentCapacity.set(capacity);
    }

    public ConcurrentMap<String, String> getKeyMap() {
        return keyMap;
    }

    public void clearKeyMap() {
        keyMap.clear();
    }

    // key, <expected value, actual value>
    private ConcurrentMap<String, Entry<String, String>> quarantinedKeyMap = Maps.newConcurrentMap();

    public ConcurrentMap<String, Entry<String, String>> getQuarantinedKeyMap() {
        return quarantinedKeyMap;
    }

    public void clearQuarantinedKeyMap() {
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

    public Entry<String, String> genRandomEntry(String key, int valueSizeThreshold) {
        if (honorCapacity.get() && currentCapacity.getValue() < 1) {
            return null;
        }
        currentCapacity.dec();

        return new AbstractMap.SimpleEntry<>(
            key,
            RandomStringUtils.randomAlphanumeric(RANDOM.nextInt(valueSizeThreshold)));
    }

    public Entry<String, String> popRandomEntry() {
        if (keyMap.isEmpty()) {
            return null;
        }

        Entry<String, String> entry;

        synchronized (this) {
            List<Entry<String, String>> array = new ArrayList<>(keyMap.entrySet());
            entry = array.get(RANDOM.nextInt(keyMap.size()));
            keyMap.remove(entry.getKey());
        }

        return entry;
    }

    public Entry<String, String> getRandomEntry() {
        if (keyMap.isEmpty()) {
            return null;
        }

        List<Entry<String, String>> array = new ArrayList<>(keyMap.entrySet());
        return array.get(RANDOM.nextInt(keyMap.size()));
    }

    public void quarantineEntry(Entry<String, String> entry, String value) {
        quarantinedKeyMap.put(entry.getKey(),
            new AbstractMap.SimpleEntry<>(entry.getValue(), value));
        keyMap.remove(entry.getKey());
    }

}
