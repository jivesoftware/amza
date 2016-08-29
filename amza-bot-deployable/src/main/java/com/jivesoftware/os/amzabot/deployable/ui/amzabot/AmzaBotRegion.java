package com.jivesoftware.os.amzabot.deployable.ui.amzabot;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amzabot.deployable.AmzaBotService;
import com.jivesoftware.os.amzabot.deployable.ui.SoyRenderer;
import com.jivesoftware.os.amzabot.deployable.ui.UiPageRegion;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;

// soy.dive.page.main
public class AmzaBotRegion implements UiPageRegion<AmzaBotInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String mainTemplate;
    private final SoyRenderer renderer;

    public AmzaBotRegion(String mainTemplate,
        SoyRenderer renderer) {

        this.mainTemplate = mainTemplate;
        this.renderer = renderer;
    }

    @Override
    public String render(final AmzaBotInput input) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            data.put("key", input.key);
        } catch (Exception x) {
            data.put("message", x.getMessage());
            LOG.error("Failure.", x);
        }

        return renderer.render(mainTemplate, data);
    }

    @Override
    public String getTitle(AmzaBotInput input) {
        return "AmzaBot";
    }

}
