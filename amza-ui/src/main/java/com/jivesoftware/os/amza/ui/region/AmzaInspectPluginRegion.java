package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.service.AmzaRegion;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
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
        final String key;
        final int offset;
        final int batchSize;
        final String action;

        public AmzaInspectPluginRegionInput(boolean systemRegion,
            String ringName,
            String regionName,
            String key,
            int offset,
            int batchSize,
            String action) {
            this.systemRegion = systemRegion;
            this.ringName = ringName;
            this.regionName = regionName;
            this.key = key;
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
        data.put("key", input.key);
        data.put("offset", String.valueOf(input.offset));
        data.put("batchSize", String.valueOf(input.batchSize));

        List<String> msg = new ArrayList<>();
        try {
            List<Map<String, String>> rows = new ArrayList<>();

            if (input.action.equals("scan")) {
                AmzaRegion amzaRegion = lookupRegion(input, msg);
                if (amzaRegion != null) {
                    final AtomicLong offset = new AtomicLong(input.offset);
                    final AtomicLong batch = new AtomicLong(input.batchSize);
                    amzaRegion.scan((long rowTxId, WALKey key, WALValue value) -> {
                        if (offset.decrementAndGet() >= 0) {
                            return true;
                        }
                        if (batch.decrementAndGet() >= 0) {
                            Map<String, String> row = new HashMap<>();
                            row.put("rowTxId", String.valueOf(rowTxId));
                            row.put("key", bytesToHex(key.getKey()));
                            row.put("value", bytesToHex(value.getValue()));
                            row.put("timestamp", String.valueOf(value.getTimestampId()));
                            row.put("tombstone", String.valueOf(value.getTombstoned()));
                            rows.add(row);
                            return true;
                        } else {
                            return false;
                        }
                    });
                }
            } else if (input.action.equals("get")) {
                AmzaRegion amzaRegion = lookupRegion(input, msg);
                if (amzaRegion != null) {
                    List<WALKey> walKeys = stringToWALKeys(input);
                    if (walKeys.isEmpty()) {
                        msg.add("No keys to get. Please specifiy a valid key. key='" + input.key + "'");
                    } else {
                        amzaRegion.get(walKeys, (long rowTxId, WALKey key, WALValue value) -> {
                            Map<String, String> row = new HashMap<>();
                            row.put("rowTxId", String.valueOf(rowTxId));
                            row.put("key", bytesToHex(key.getKey()));
                            row.put("value", bytesToHex(value.getValue()));
                            row.put("timestamp", String.valueOf(value.getTimestampId()));
                            row.put("tombstone", String.valueOf(value.getTombstoned()));
                            rows.add(row);
                            return true;
                        });
                    }
                }
            } else if (input.action.equals("remove")) {
                AmzaRegion amzaRegion = lookupRegion(input, msg);
                if (amzaRegion != null) {
                    List<WALKey> walKeys = stringToWALKeys(input);
                    if (walKeys.isEmpty()) {
                        msg.add("No keys to remove. Please specifiy a valid key. key='" + input.key + "'");
                    } else {
                        amzaRegion.remove(walKeys);
                        amzaRegion.get(walKeys, (long rowTxId, WALKey key, WALValue value) -> {
                            Map<String, String> row = new HashMap<>();
                            row.put("rowTxId", String.valueOf(rowTxId));
                            row.put("key", bytesToHex(key.getKey()));
                            row.put("value", bytesToHex(value.getValue()));
                            row.put("timestamp", String.valueOf(value.getTimestampId()));
                            row.put("tombstone", String.valueOf(value.getTombstoned()));
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

    private List<WALKey> stringToWALKeys(AmzaInspectPluginRegionInput input) {
        String[] keys = input.key.split(",");
        List<WALKey> walKeys = new ArrayList<>();
        for (String key : keys) {
            byte[] rawKey = hexStringToByteArray(key.trim());
            walKeys.add(new WALKey(rawKey));
        }
        return walKeys;
    }

    private AmzaRegion lookupRegion(AmzaInspectPluginRegionInput input, List<String> msg) throws Exception {
        AmzaRegion region = amzaService.getRegion(new RegionName(input.systemRegion, input.ringName, input.regionName));
        if (region == null) {
            msg.add("No region for ringName:" + input.ringName + " regionName:" + input.regionName + " isSystem:" + input.systemRegion);
        }
        return region;
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
