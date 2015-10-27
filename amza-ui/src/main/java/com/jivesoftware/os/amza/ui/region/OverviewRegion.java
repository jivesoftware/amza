package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.ui.region.OverviewRegion.OverviewInput;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import java.util.Map;

/**
 *
 */
public class OverviewRegion implements PageRegion<OverviewInput> {

    private final String template;
    private final SoyRenderer renderer;

    public OverviewRegion(String template, SoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    public static class OverviewInput {

        public OverviewInput() {
        }
    }

    @Override
    public String render(OverviewInput input) {
        Map<String, Object> data = Maps.newHashMap();
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Overview";
    }

}
