package com.jivesoftware.os.amzabot.deployable;

import com.google.common.collect.Maps;
import com.jivesoftware.os.mlogger.core.AtomicCounter;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.AbstractMap;
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
    private ConcurrentMap<String, String> keyMap = Maps.newConcurrentMap();

    AmzaKeyClearingHouse() {
    }

    AmzaKeyClearingHouse(long capacity) {
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

    public String genRandomValue(int valueSizeThreshold) {
        return RandomStringUtils.randomAlphanumeric(RANDOM.nextInt(valueSizeThreshold));
    }

    public Entry<String, String> genRandomEntry(String key, int valueSizeThreshold) {
        if (honorCapacity.get() && currentCapacity.getValue() < 1) {
            return null;
        }
        currentCapacity.dec();

        return new AbstractMap.SimpleEntry<>(
            key,
            genRandomValue(valueSizeThreshold));
    }

    public Entry<String, String> popRandomEntry() {
        if (keyMap.isEmpty()) {
            return null;
        }

        List<Entry<String, String>> array = new ArrayList<>(keyMap.entrySet());
        Entry<String, String> entry = array.get(RANDOM.nextInt(keyMap.size()));
        keyMap.remove(entry.getKey());

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

    public boolean verifyKeyMap(Map<String, String> partitionMap) throws Exception {
        boolean res = true;

        LOG.info("Verify key map with partition snapshot of size {}.", partitionMap.size());

        Map<String, String> keyMapCopy = new HashMap<>(keyMap);

        for (Entry<String, String> entry : partitionMap.entrySet()) {
            String value = keyMapCopy.remove(entry.getKey());

            if (value == null) {
                quarantineEntry(entry, null);
                LOG.error("Key's value not found {}:{}:{}",
                    entry.getKey(),
                    AmzaBotUtil.truncVal(entry.getValue()),
                    null);

                res = false;
            } else if (!value.equals(entry.getValue())) {
                quarantineEntry(entry, value);
                LOG.error("Key's value differs {}:{}:{}",
                    entry.getKey(),
                    AmzaBotUtil.truncVal(entry.getValue()),
                    AmzaBotUtil.truncVal(value));

                res = false;
            }
        }

        for (Entry<String, String> entry : keyMapCopy.entrySet()) {
            quarantineEntry(entry, "extra");
            LOG.error("Extra key found in clearing house {}:{}",
                entry.getKey(),
                AmzaBotUtil.truncVal(entry.getValue()));

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
