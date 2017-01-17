package com.jivesoftware.os.amza.sync.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncSender;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncSenders;
import com.jivesoftware.os.amza.ui.region.Region;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 */
public class AmzaStatusFocusRegion implements Region<AmzaStatusRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaSyncSenders syncSenders;
    private final ObjectMapper mapper;

    public AmzaStatusFocusRegion(String template,
        SoyRenderer renderer,
        AmzaSyncSenders syncSenders,
        ObjectMapper mapper) {

        this.template = template;
        this.renderer = renderer;
        this.syncSenders = syncSenders;
        this.mapper = mapper;
    }

    @Override
    public String render(AmzaStatusRegionInput input) {

        String syncspaceName = input.syncspaceName;
        PartitionName partitionName = input.partitionName;

        Map<String, Object> data = Maps.newHashMap();
        data.put("syncspaceName", syncspaceName);
        data.put("partitionBase64", partitionName.toBase64());
        try {
            List<Map<String, Object>> progress = Lists.newArrayList();
            if (syncSenders != null) {
                AmzaSyncSender syncSender = syncSenders.getSender(input.syncspaceName);
                if (syncSender != null) {
                    syncSender.streamCursors(partitionName, null, (toPartitionName, cursor) -> {
                        String toPartition = new String(toPartitionName.getRingName(), StandardCharsets.UTF_8)
                            + '/' + new String(toPartitionName.getName(), StandardCharsets.UTF_8);
                        Map<String, String> cursorData = Maps.newHashMap();
                        for (Entry<RingMember, Long> entry : cursor.entrySet()) {
                            cursorData.put(entry.getKey().getMember(), entry.getValue().toString());
                        }

                        progress.add(ImmutableMap.of(
                            "sender", syncSender.getConfig().name,
                            "toPartition", toPartition,
                            "cursor", mapper.writeValueAsString(cursorData)));
                        return true;
                    });
                }
            } else {
                data.put("warning", "Sync sender is not enabled");
            }
            data.put("progress", progress);
        } catch (Exception e) {
            log.error("Unable to get progress for syncspace:{} partition:{}", new Object[] { input.syncspaceName, input.partitionName }, e);
        }

        return renderer.render(template, data);
    }
}
