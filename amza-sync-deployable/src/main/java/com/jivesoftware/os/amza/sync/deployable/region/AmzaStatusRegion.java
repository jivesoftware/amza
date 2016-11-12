package com.jivesoftware.os.amza.sync.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.ui.region.PageRegion;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 *
 */
public class AmzaStatusRegion implements PageRegion<PartitionName> {

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaStatusFocusRegion statusFocusRegion;

    public AmzaStatusRegion(String template, SoyRenderer renderer, AmzaStatusFocusRegion statusFocusRegion) {
        this.template = template;
        this.renderer = renderer;
        this.statusFocusRegion = statusFocusRegion;
    }

    @Override
    public String render(PartitionName partitionName) {
        Map<String, Object> data = Maps.newHashMap();
        if (partitionName != null) {
            data.put("ringName", new String(partitionName.getRingName(), StandardCharsets.UTF_8));
            data.put("partitionName", new String(partitionName.getName(), StandardCharsets.UTF_8));
            data.put("statusFocusRegion", statusFocusRegion.render(partitionName));
        }
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Status";
    }
}
