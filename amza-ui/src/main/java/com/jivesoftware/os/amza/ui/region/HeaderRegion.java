package com.jivesoftware.os.amza.ui.region;

import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import java.util.Map;

// soy.chrome.headerRegion
public class HeaderRegion implements Region<Map<String, ?>> {

    private final String template;
    private final SoyRenderer renderer;

    public HeaderRegion(String template, SoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Map<String, ?> input) {
        return renderer.render(template, input);
    }
}
