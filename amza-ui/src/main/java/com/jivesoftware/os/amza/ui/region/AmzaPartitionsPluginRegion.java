package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedState;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.replication.PartitionStateStorage;
import com.jivesoftware.os.amza.service.replication.PartitionStripe;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.amza.ui.region.AmzaPartitionsPluginRegion.AmzaPartitionsPluginRegionInput;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// soy.page.amzaPartitionsPluginRegion
public class AmzaPartitionsPluginRegion implements PageRegion<AmzaPartitionsPluginRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private final NumberFormat numberFormat = NumberFormat.getInstance();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaRingReader ringReader;
    private final AmzaService amzaService;

    public AmzaPartitionsPluginRegion(String template,
        SoyRenderer renderer,
        AmzaRingReader ringReader,
        AmzaService amzaService) {
        this.template = template;
        this.renderer = renderer;
        this.ringReader = ringReader;
        this.amzaService = amzaService;
    }

    public static class AmzaPartitionsPluginRegionInput {

        final String action;
        final String ringName;
        final String partitionName;
        final String consistency;
        final boolean requireConsistency;
        final int takeFromFactor;

        public AmzaPartitionsPluginRegionInput(String action,
            String ringName,
            String partitionName,
            String consistency,
            boolean requireConsistency,
            int takeFromFactor) {
            this.action = action;
            this.ringName = ringName;
            this.partitionName = partitionName;
            this.consistency = consistency;
            this.requireConsistency = requireConsistency;
            this.takeFromFactor = takeFromFactor;
        }

    }

    @Override
    public String render(AmzaPartitionsPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        try {

            byte[] ringNameBytes = input.ringName.getBytes();
            byte[] partitionNameBytes = input.partitionName.getBytes();
            if (input.action.equals("add")) {
                if (ringNameBytes.length > 0 && partitionNameBytes.length > 0) {
                    int ringSize = amzaService.getRingReader().getRingSize(ringNameBytes);
                    int systemRingSize = amzaService.getRingReader().getRingSize(AmzaRingReader.SYSTEM_RING);
                    if (ringSize < systemRingSize) {
                        amzaService.getRingWriter().buildRandomSubRing(ringNameBytes, systemRingSize);
                    }

                    WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(new PrimaryIndexDescriptor("berkeleydb", 0, false, null),
                        null, 1000, 1000);

                    PartitionName partitionName = new PartitionName(false, ringNameBytes, partitionNameBytes);
                    amzaService.setPropertiesIfAbsent(partitionName,
                        new PartitionProperties(storageDescriptor,
                            Consistency.valueOf(input.consistency),
                            input.requireConsistency,
                            input.takeFromFactor,
                            false));
                    amzaService.awaitOnline(partitionName, TimeUnit.SECONDS.toMillis(30));
                }
            } else if (input.action.equals("promote")) {
                if (ringNameBytes.length > 0 && partitionNameBytes.length > 0) {
                    PartitionName partitionName = new PartitionName(false, ringNameBytes, partitionNameBytes);
                    log.info("Promoting {}", partitionName);
                    amzaService.promotePartition(partitionName);
                }
            } else if (input.action.equals("remove")) {
                if (ringNameBytes.length > 0 && partitionNameBytes.length > 0) {
                    PartitionName partitionName = new PartitionName(false, ringNameBytes, partitionNameBytes);
                    log.info("Removing {}", partitionName);
                    amzaService.destroyPartition(partitionName);
                }
            }
            long now = System.currentTimeMillis();
            List<Map<String, Object>> rows = new ArrayList<>();
            Set<PartitionName> allPartitionNames = amzaService.getPartitionNames();
            List<PartitionName> partitionNames = Lists.newArrayList(allPartitionNames);
            Collections.sort(partitionNames, (o1, o2) -> {
                int i = -Boolean.compare(o1.isSystemPartition(), o2.isSystemPartition());
                if (i != 0) {
                    return i;
                }
                i = UnsignedBytes.lexicographicalComparator().compare(o1.getRingName(), o2.getRingName());
                if (i != 0) {
                    return i;
                }
                i = UnsignedBytes.lexicographicalComparator().compare(o1.getName(), o2.getName());
                if (i != 0) {
                    return i;
                }
                return i;
            });
            for (PartitionName partitionName : partitionNames) {

                Map<String, Object> row = new HashMap<>();
                VersionedState state = amzaService.getPartitionStateStorage().getLocalVersionedState(partitionName);
                LivelyEndState livelyEndState = state.getLivelyEndState();

                row.put("alive", state == null ? "unknown" : livelyEndState.currentWaterline.isAlive(now));
                row.put("state", state == null ? "unknown" : livelyEndState.currentWaterline.getState());
                row.put("quorum", state == null ? "unknown" : livelyEndState.currentWaterline.isAtQuorum());
                row.put("timestamp", state == null ? "unknown" : String.valueOf(livelyEndState.currentWaterline.getTimestamp()));
                row.put("version", state == null ? "unknown" : String.valueOf(livelyEndState.currentWaterline.getVersion()));

                row.put("storageVersion", state == null ? "unknown" : Long.toHexString(state.getPartitionVersion()));
                row.put("type", partitionName.isSystemPartition() ? "SYSTEM" : "USER");
                row.put("name", new String(partitionName.getName()));
                row.put("ringName", new String(partitionName.getRingName()));

                NavigableMap<RingMember, RingHost> ring = ringReader.getRing(partitionName.getRingName());
                List<Map<String, Object>> ringHosts = new ArrayList<>();
                for (Map.Entry<RingMember, RingHost> r : ring.entrySet()) {
                    Map<String, Object> ringHost = new HashMap<>();
                    ringHost.put("member", r.getKey().getMember());
                    ringHost.put("host", r.getValue().getHost());
                    ringHost.put("port", String.valueOf(r.getValue().getPort()));
                    ringHosts.add(ringHost);
                }
                row.put("ring", ringHosts);

                PartitionProperties partitionProperties = amzaService.getPartitionProperties(partitionName);
                if (partitionProperties == null) {
                    row.put("disabled", "?");
                    row.put("consistency", "none");
                    row.put("requireConsistency", "true");
                    row.put("takeFromFactor", "?");

                    WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                        new PrimaryIndexDescriptor("memory", 0, false, null), null, 1000, 1000);
                    row.put("walStorageDescriptor", walStorageDescriptor(storageDescriptor));

                } else {
                    row.put("disabled", partitionProperties.disabled);
                    row.put("takeFromFactor", partitionProperties.takeFromFactor);
                    row.put("consistency", partitionProperties.consistency.name());
                    row.put("requireConsistency", partitionProperties.requireConsistency);
                    row.put("walStorageDescriptor", walStorageDescriptor(partitionProperties.walStorageDescriptor));
                }

                PartitionStateStorage partitionStateStorage = amzaService.getPartitionStateStorage();
                VersionedState localState = partitionStateStorage.getLocalVersionedState(partitionName);
                VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, localState.getPartitionVersion());

                if (partitionName.isSystemPartition()) {
                    HighwaterStorage systemHighwaterStorage = amzaService.getSystemHighwaterStorage();
                    WALHighwater partitionHighwater = systemHighwaterStorage.getPartitionHighwater(versionedPartitionName);
                    row.put("highwaters", renderHighwaters(partitionHighwater));
                } else {
                    PartitionStripeProvider partitionStripeProvider = amzaService.getPartitionStripeProvider();
                    partitionStripeProvider.txPartition(partitionName, (PartitionStripe stripe, HighwaterStorage highwaterStorage) -> {
                        WALHighwater partitionHighwater = highwaterStorage.getPartitionHighwater(versionedPartitionName);
                        row.put("highwaters", renderHighwaters(partitionHighwater));
                        return null;
                    });
                }

                rows.add(row);
            }

            data.put("partitions", rows);

        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    public String renderHighwaters(WALHighwater walHighwater) {
        StringBuilder sb = new StringBuilder();
        for (WALHighwater.RingMemberHighwater e : walHighwater.ringMemberHighwater) {
            sb.append("<p>");
            sb.append(e.ringMember.getMember()).append("=").append(Long.toHexString(e.transactionId)).append("\n");
            sb.append("</p>");
        }
        return sb.toString();
    }

    Map<String, Object> walStorageDescriptor(WALStorageDescriptor storageDescriptor) {
        Map<String, Object> map = new HashMap<>();
        map.put("maxUpdatesBetweenCompactionHintMarker", numberFormat.format(storageDescriptor.maxUpdatesBetweenCompactionHintMarker));
        map.put("maxUpdatesBetweenIndexCommitMarker", numberFormat.format(storageDescriptor.maxUpdatesBetweenIndexCommitMarker));
        map.put("primaryIndexDescriptor", primaryIndexDescriptor(storageDescriptor.primaryIndexDescriptor));

        List<Map<String, Object>> secondary = new ArrayList<>();
        if (storageDescriptor.secondaryIndexDescriptors != null) {
            for (SecondaryIndexDescriptor secondaryIndexDescriptor : storageDescriptor.secondaryIndexDescriptors) {
                secondary.add(secondaryIndexDescriptor(secondaryIndexDescriptor));
            }
        }
        map.put("secondaryIndexDescriptor", secondary);
        return map;
    }

    Map<String, Object> primaryIndexDescriptor(PrimaryIndexDescriptor primaryIndexDescriptor) {

        Map<String, Object> map = new HashMap<>();
        map.put("className", primaryIndexDescriptor.className);
        map.put("ttlInMillis", numberFormat.format(primaryIndexDescriptor.ttlInMillis));
        map.put("forceCompactionOnStartup", primaryIndexDescriptor.forceCompactionOnStartup);
        //public Map<String, String> properties; //TODO
        return map;
    }

    Map<String, Object> secondaryIndexDescriptor(SecondaryIndexDescriptor secondaryIndexDescriptor) {
        Map<String, Object> map = new HashMap<>();
        map.put("className", secondaryIndexDescriptor.className);
        //public Map<String, String> properties; //TODO
        return map;
    }

    @Override
    public String getTitle() {
        return "Amza Partitions";
    }

}
