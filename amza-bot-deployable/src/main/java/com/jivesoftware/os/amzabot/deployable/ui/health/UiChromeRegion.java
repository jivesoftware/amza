package com.jivesoftware.os.amzabot.deployable.ui.health;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amzabot.deployable.ui.SoyRenderer;
import com.jivesoftware.os.amzabot.deployable.ui.UiPageRegion;
import com.jivesoftware.os.amzabot.deployable.ui.UiRegion;
import java.util.Map;

// soy.ui.chrome.chromeRegion
public class UiChromeRegion<I, R extends UiPageRegion<I>> implements UiRegion<I> {

    private final String cacheToken;
    private final String template;
    private final SoyRenderer renderer;
    private final UiHeaderRegion headerRegion;
    private final R region;
    private final String version;

    public UiChromeRegion(String cacheToken, String template, SoyRenderer renderer, UiHeaderRegion headerRegion, R region) {
        this.cacheToken = cacheToken;
        this.template = template;
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.region = region;
        this.version = buildVersion();
    }

    @Override
    public String render(I input) {
        Map<String, Object> data = Maps.newHashMap();
        data.put("cacheToken", cacheToken);
        data.put("header", headerRegion.render(null));
        data.put("region", region.render(input));
        data.put("title", region.getTitle(input));
        data.put("version", version);
        return renderer.render(template, data);

    }

    private String buildVersion() {
        return "unknown";
    }

}
