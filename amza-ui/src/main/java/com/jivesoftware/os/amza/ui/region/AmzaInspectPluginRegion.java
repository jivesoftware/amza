package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPI;
import com.jivesoftware.os.amza.shared.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
// soy.page.amzaStressPluginRegion
public class AmzaInspectPluginRegion implements PageRegion<AmzaInspectPluginRegion.AmzaInspectPluginRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaService amzaService;

    public AmzaInspectPluginRegion(String template,
        SoyRenderer renderer,
        AmzaService amzaService) {
        this.template = template;
        this.renderer = renderer;
        this.amzaService = amzaService;
    }

    public static class AmzaInspectPluginRegionInput {

        final boolean systemRegion;
        final String ringName;
        final String regionName;
        final String prefix;
        final String key;
        final String toPrefix;
        final String toKey;
        final String value;
        final int offset;
        final int batchSize;
        final String action;

        public AmzaInspectPluginRegionInput(boolean systemRegion,
            String ringName,
            String regionName,
            String prefix,
            String key,
            String toPrefix,
            String toKey,
            String value, int offset,
            int batchSize,
            String action) {
            this.systemRegion = systemRegion;
            this.ringName = ringName;
            this.regionName = regionName;
            this.prefix = prefix;
            this.key = key;
            this.toPrefix = toPrefix;
            this.toKey = toKey;
            this.value = value;
            this.offset = offset;
            this.batchSize = batchSize;
            this.action = action;
        }
    }

    @Override
    public String render(AmzaInspectPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        data.put("systemRegion", input.systemRegion);
        data.put("ringName", input.ringName);
        data.put("regionName", input.regionName);
        data.put("prefix", input.prefix);
        data.put("key", input.key);
        data.put("toPrefix", input.toPrefix);
        data.put("toKey", input.toKey);
        data.put("value", input.value);
        data.put("offset", String.valueOf(input.offset));
        data.put("batchSize", String.valueOf(input.batchSize));

        List<String> msg = new ArrayList<>();
        try {
            List<Map<String, String>> rows = new ArrayList<>();

            if (input.action.equals("scan")) {
                AmzaPartitionAPI partition = lookupPartition(input, msg);
                if (partition != null) {
                    final AtomicLong offset = new AtomicLong(input.offset);
                    final AtomicLong batch = new AtomicLong(input.batchSize);
                    partition.scan(getPrefix(input.prefix),
                        input.key.isEmpty() ? null : hexStringToByteArray(input.key),
                        getPrefix(input.toPrefix),
                        input.toKey.isEmpty() ? null : hexStringToByteArray(input.toKey),
                        (rowTxId, prefix, key, value) -> {
                            if (offset.decrementAndGet() >= 0) {
                                return true;
                            }
                            if (batch.decrementAndGet() >= 0) {
                                Map<String, String> row = new HashMap<>();
                                row.put("rowTxId", String.valueOf(rowTxId));
                                row.put("prefixAsHex", bytesToHex(prefix));
                                row.put("prefixAsString", prefix != null ? new String(prefix, StandardCharsets.US_ASCII) : "");
                                row.put("keyAsHex", bytesToHex(key));
                                row.put("keyAsString", new String(key, StandardCharsets.US_ASCII));
                                row.put("valueAsHex", bytesToHex(value.getValue()));
                                row.put("valueAsString", value != null ? new String(value.getValue(), StandardCharsets.US_ASCII) : "null");
                                row.put("timestampAsHex", Long.toHexString(value.getTimestampId()));
                                row.put("timestamp", String.valueOf(value.getTimestampId()));
                                row.put("tombstone", "false");
                                rows.add(row);
                                return true;
                            } else {
                                return false;
                            }
                        });
                }
            } else if (input.action.equals("get")) {
                AmzaPartitionAPI partition = lookupPartition(input, msg);
                if (partition != null) {
                    List<byte[]> rawKeys = stringToWALKeys(input.key);
                    if (rawKeys.isEmpty()) {
                        msg.add("No keys to get. Please specifiy a valid key. key='" + input.key + "'");
                    } else {
                        partition.get(getPrefix(input.prefix),
                            walKeysFromList(rawKeys),
                            (prefix, key, value, timestamp) -> {
                                Map<String, String> row = new HashMap<>();
                                row.put("prefixAsHex", bytesToHex(prefix));
                                row.put("prefixAsString", prefix != null ? new String(prefix, StandardCharsets.US_ASCII) : "");
                                row.put("keyAsHex", bytesToHex(key));
                                row.put("keyAsString", new String(key, StandardCharsets.US_ASCII));
                                row.put("valueAsHex", bytesToHex(value));
                                row.put("valueAsString", value != null ? new String(value, StandardCharsets.US_ASCII) : "null");
                                row.put("timestampAsHex", Long.toHexString(timestamp));
                                row.put("timestamp", String.valueOf(timestamp));
                                row.put("tombstone", "false");
                                rows.add(row);
                                return true;
                            });
                    }
                }
            } else if (input.action.equals("set")) {
                AmzaPartitionAPI partition = lookupPartition(input, msg);
                if (partition != null) {
                    List<byte[]> rawKeys = stringToWALKeys(input.key);
                    if (rawKeys.isEmpty()) {
                        msg.add("No keys to remove. Please specifiy a valid key. key='" + input.key + "'");
                    } else {
                        AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
                        for (byte[] rawKey : rawKeys) {
                            updates.set(rawKey, hexStringToByteArray(input.value));
                        }
                        //TODO expose to UI
                        partition.commit(getPrefix(input.prefix), updates, 1, 30_000);
                        partition.get(getPrefix(input.prefix), walKeysFromList(rawKeys), (prefix, key, value, timestamp) -> {
                            Map<String, String> row = new HashMap<>();
                            row.put("prefixAsHex", bytesToHex(prefix));
                            row.put("prefixAsString", prefix != null ? new String(prefix, StandardCharsets.US_ASCII) : "");
                            row.put("keyAsHex", bytesToHex(key));
                            row.put("keyAsString", new String(key, StandardCharsets.US_ASCII));
                            row.put("valueAsHex", bytesToHex(value));
                            row.put("valueAsString", value != null ? new String(value, StandardCharsets.US_ASCII) : "null");
                            row.put("timestampAsHex", Long.toHexString(timestamp));
                            row.put("timestamp", String.valueOf(timestamp));
                            row.put("tombstone", "false");
                            rows.add(row);
                            return true;
                        });
                    }
                }
            } else if (input.action.equals("remove")) {
                AmzaPartitionAPI partition = lookupPartition(input, msg);
                if (partition != null) {
                    List<byte[]> fromRawKeys = stringToWALKeys(input.key);
                    List<byte[]> toRawKeys = stringToWALKeys(input.key);
                    if (fromRawKeys.isEmpty()) {
                        msg.add("No keys to remove. Please specifiy a valid key. key='" + input.key + "'");
                    } else if (fromRawKeys.size() > 1 && !toRawKeys.isEmpty() || toRawKeys.size() > 1) {
                        msg.add("You can't mix comma-separation and key ranges. Please specifiy a valid key or range." +
                            " key='" + input.key + "'" +
                            " toKey='" + input.toKey + "'");
                    } else if (!fromRawKeys.isEmpty() && !toRawKeys.isEmpty()) {
                        AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
                        byte[][] lastPrefix = new byte[1][];
                        partition.scan(getPrefix(input.prefix),
                            fromRawKeys.get(0),
                            getPrefix(input.toPrefix),
                            toRawKeys.get(0),
                            (rowTxId, prefix, key, scanned) -> {
                                if (updates.size() >= input.batchSize || !Arrays.equals(lastPrefix[0], prefix)) {
                                    partition.commit(lastPrefix[0], updates, 1, 30_000);
                                    updates.reset();
                                }
                                lastPrefix[0] = prefix;
                                updates.remove(key);
                                return true;
                            });
                        if (updates.size() > 0) {
                            partition.commit(lastPrefix[0], updates, 1, 30_000);
                        }
                    } else {
                        AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
                        for (byte[] rawKey : fromRawKeys) {
                            updates.remove(rawKey, -1);
                        }
                        //TODO expose to UI
                        partition.commit(getPrefix(input.prefix), updates, 1, 30_000);
                        partition.get(getPrefix(input.prefix), walKeysFromList(fromRawKeys), (prefix, key, value, timestamp) -> {
                            Map<String, String> row = new HashMap<>();
                            row.put("prefixAsHex", bytesToHex(prefix));
                            row.put("prefixAsString", prefix != null ? new String(prefix, StandardCharsets.US_ASCII) : "");
                            row.put("keyAsHex", bytesToHex(key));
                            row.put("keyAsString", new String(key, StandardCharsets.US_ASCII));
                            row.put("valueAsHex", bytesToHex(value));
                            row.put("valueAsString", value != null ? new String(value, StandardCharsets.US_ASCII) : "null");
                            row.put("timestampAsHex", Long.toHexString(timestamp));
                            row.put("timestamp", String.valueOf(timestamp));
                            row.put("tombstone", "false");
                            rows.add(row);
                            return true;
                        });
                    }
                }
            }
            data.put("rows", rows);

        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
            msg.add(e.getMessage());
        }

        data.put("msg", msg);

        return renderer.render(template, data);
    }

    private byte[] getPrefix(String prefix) {
        return prefix.isEmpty() ? null : hexStringToByteArray(prefix);
    }

    private UnprefixedWALKeys walKeysFromList(List<byte[]> rawKeys) {
        return stream -> {
            for (byte[] key : rawKeys) {
                if (!stream.stream(key)) {
                    return false;
                }
            }
            return true;
        };
    }

    private List<byte[]> stringToWALKeys(String keyString) {
        String[] keys = keyString.split(",");
        List<byte[]> walKeys = new ArrayList<>();
        for (String key : keys) {
            String trimmed = key.trim();
            if (!trimmed.isEmpty()) {
                byte[] rawKey = hexStringToByteArray(trimmed);
                walKeys.add(rawKey);
            }
        }
        return walKeys;
    }

    private AmzaPartitionAPI lookupPartition(AmzaInspectPluginRegionInput input, List<String> msg) throws Exception {
        AmzaPartitionAPI partition = amzaService.getPartition(new PartitionName(input.systemRegion, input.ringName.getBytes(), input.regionName.getBytes()));
        if (partition == null) {
            msg.add("No region for ringName:" + input.ringName + " regionName:" + input.regionName + " isSystem:" + input.systemRegion);
        }
        return partition;
    }

    @Override
    public String getTitle() {
        return "Amza Inspect";
    }

    public static byte[] hexStringToByteArray(String s) {
        if (s.equals("null")) {
            return null;
        }
        if (s.length() == 0) {
            return new byte[0];
        }
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

}
