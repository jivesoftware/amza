package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.service.replication.HostRingBuilder;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.HostRing;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.MemoryFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AmzaRingReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final RingHost ringHost;
    private final RegionIndex regionIndex;

    public AmzaRingReader(RingHost ringHost, RegionIndex regionIndex) {
        this.ringHost = ringHost;
        this.regionIndex = regionIndex;
    }

    WALKey key(String ringName, RingHost ringHost) throws IOException {
        MemoryFiler filer = new MemoryFiler();
        UIO.writeString(filer, ringName.toUpperCase(), "ringName");
        UIO.writeByte(filer, 0, "seperator");
        if (ringHost != null) {
            UIO.writeByteArray(filer, ringHost.toBytes(), "ringHost");
        }
        return new WALKey(filer.getBytes());
    }

    RingHost keyToRingHost(WALKey key) throws IOException {
        MemoryFiler filer = new MemoryFiler(key.getKey());
        UIO.readString(filer, "ringName");
        UIO.readByte(filer, "seperator");
        return RingHost.fromBytes(UIO.readByteArray(filer, "ringHost"));
    }

    String keyToRingName(WALKey key) throws IOException {
        MemoryFiler filer = new MemoryFiler(key.getKey());
        return UIO.readString(filer, "ringName");
    }

    public RingHost getRingHost() {
        return ringHost;
    }

    public HostRing getHostRing(String ringName) throws Exception {
        return new HostRingBuilder().build(ringHost, getRing(ringName));
    }

    public List<RingHost> getRing(String ringName) throws Exception {
        RegionStore ringIndex = regionIndex.get(RegionProvider.RING_INDEX);
        if (ringIndex == null) {
            LOG.warn("Attempting to get ring before ring index has been initialized.");
            return Collections.emptyList();
        }

        final Set<RingHost> ringHosts = new HashSet<>();
        WALKey from = key(ringName, null);
        ringIndex.rangeScan(from, from.prefixUpperExclusive(), (long orderId, WALKey key, WALValue value) -> {
            ringHosts.add(keyToRingHost(key));
            return true;
        });
        if (ringHosts.isEmpty()) {
            return new ArrayList<>();
        } else {
            return new ArrayList<>(ringHosts);
        }
    }

}
