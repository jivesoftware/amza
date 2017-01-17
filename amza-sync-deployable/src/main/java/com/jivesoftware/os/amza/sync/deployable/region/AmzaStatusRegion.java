package com.jivesoftware.os.amza.sync.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncSenders;
import com.jivesoftware.os.amza.ui.region.PageRegion;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class AmzaStatusRegion implements PageRegion<AmzaStatusRegionInput> {

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaStatusFocusRegion statusFocusRegion;
    private final AmzaSyncSenders syncSenders;

    public AmzaStatusRegion(String template,
        SoyRenderer renderer,
        AmzaStatusFocusRegion statusFocusRegion,
        AmzaSyncSenders syncSenders) {
        this.template = template;
        this.renderer = renderer;
        this.statusFocusRegion = statusFocusRegion;
        this.syncSenders = syncSenders;
    }

    @Override
    public String render(AmzaStatusRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();
        data.put("syncspaces", syncSenders.getSyncspaces());
        if (input != null) {
            data.put("syncspaceName", input.syncspaceName);
            data.put("ringName", new String(input.partitionName.getRingName(), StandardCharsets.UTF_8));
            data.put("partitionName", new String(input.partitionName.getName(), StandardCharsets.UTF_8));
            data.put("statusFocusRegion", statusFocusRegion.render(input));
        }
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Status";
    }
}
