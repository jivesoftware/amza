package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.ui.region.HomeRegion.HomeInput;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;

/**
 *
 */
public class HomeRegion implements PageRegion<HomeInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final SoyRenderer renderer;

    public HomeRegion(String template, SoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    public static class HomeInput {

        final String wgetURL;
        final String amzaClusterName;

        public HomeInput(String wgetURL, String amzaClusterName) {
            this.wgetURL = wgetURL;
            this.amzaClusterName = amzaClusterName;
        }

    }

    @Override
    public String render(HomeInput input) {
        Map<String, Object> data = Maps.newHashMap();
        data.put("wgetURL", input.wgetURL);
        data.put("amzaClusterName", input.amzaClusterName);
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Home";
    }

}
