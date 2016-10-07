package com.jivesoftware.os.amzabot.deployable;

import com.google.common.collect.Maps;
import com.jivesoftware.os.mlogger.core.AtomicCounter;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.RandomStringUtils;

public class AmzaKeyClearingHouse {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Random RANDOM = new Random();

    private final AtomicBoolean honorCapacity = new AtomicBoolean(false);
    private final AtomicCounter currentCapacity = new AtomicCounter();

    // key, value
    private ConcurrentMap<String, Integer> keyMap = Maps.newConcurrentMap();

    AmzaKeyClearingHouse() {
    }

    AmzaKeyClearingHouse(long capacity) {
        honorCapacity.set(true);
        currentCapacity.set(capacity);
    }

    public ConcurrentMap<String, Integer> getKeyMap() {
        return keyMap;
    }

    public void clearKeyMap() {
        keyMap.clear();
    }

    // key, <expected value, actual value>
    private ConcurrentMap<String, Entry<Integer, Integer>> quarantinedKeyMap = Maps.newConcurrentMap();

    public ConcurrentMap<String, Entry<Integer, Integer>> getQuarantinedKeyMap() {
        return quarantinedKeyMap;
    }

    public void clearQuarantinedKeyMap() {
        quarantinedKeyMap.clear();
    }

    public Integer set(String key, String value) {
        return keyMap.put(key, value.hashCode());
    }

    public Integer get(String key) {
        return keyMap.get(key);
    }

    public Integer delete(String key) {
        return keyMap.remove(key);
    }

    public String genRandomValue(int valueSizeThreshold) {
        return RandomStringUtils.randomAlphanumeric(RANDOM.nextInt(valueSizeThreshold));
    }

    public Entry<String, String> genRandomEntry(String key, int valueSizeThreshold) {
        if (honorCapacity.get() && currentCapacity.getValue() < 1) {
            return null;
        }
        currentCapacity.dec();

        return new SimpleEntry<>(
            key,
            genRandomValue(valueSizeThreshold));
    }

    public Entry<String, Integer> popRandomEntry() {
        if (keyMap.isEmpty()) {
            return null;
        }

        List<Entry<String, Integer>> array = new ArrayList<>(keyMap.entrySet());
        Entry<String, Integer> entry = array.get(RANDOM.nextInt(keyMap.size()));
        keyMap.remove(entry.getKey());

        return entry;
    }

    public Entry<String, Integer> getRandomEntry() {
        if (keyMap.isEmpty()) {
            return null;
        }

        List<Entry<String, Integer>> array = new ArrayList<>(keyMap.entrySet());
        return array.get(RANDOM.nextInt(keyMap.size()));
    }

    public void quarantineEntry(Entry<String, Integer> entry, Integer value) {
        quarantinedKeyMap.put(entry.getKey(),
            new SimpleEntry<>(entry.getValue().hashCode(), value));
        keyMap.remove(entry.getKey());
    }

    public boolean verifyKeyMap(Map<String, String> partitionMap) throws Exception {
        boolean res = true;

        LOG.info("Verify key map with partition snapshot of size {}.", partitionMap.size());

        Map<String, Integer> keyMapCopy = new HashMap<>(keyMap);

        for (Entry<String, String> entry : partitionMap.entrySet()) {
            Integer value = keyMapCopy.remove(entry.getKey());

            if (value == null) {
                quarantineEntry(new SimpleEntry<>(entry.getKey(), entry.getValue().hashCode()), null);
                LOG.error("Key's value not found {}:{}:{}",
                    entry.getKey(),
                    AmzaBotUtil.truncVal(entry.getValue()),
                    null);

                res = false;
            } else if (entry.getValue().hashCode() != value) {
                quarantineEntry(new SimpleEntry<>(entry.getKey(), entry.getValue().hashCode()), value);
                LOG.error("Key's value differs {}:{}:{}",
                    entry.getKey(),
                    entry.getValue().hashCode(),
                    value);

                res = false;
            }
        }

        for (Entry<String, Integer> entry : keyMapCopy.entrySet()) {
            quarantineEntry(entry, -1);
            LOG.error("Extra key found in clearing house {}:{}",
                entry.getKey(),
                entry.getValue());

            res = false;
        }

        if (res) {
            LOG.info("Clearing house is clean");
        } else {
            LOG.error("Clearing house is unclean");
        }

        return res;
    }
}
