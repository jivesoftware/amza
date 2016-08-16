package com.jivesoftware.os.amzabot.deployable.ui.health;

import com.jivesoftware.os.amzabot.deployable.ui.SoyRenderer;
import com.jivesoftware.os.amzabot.deployable.ui.UiRegion;
import java.util.Collections;

// soy.ui.chrome.headerRegion
public class UiHeaderRegion implements UiRegion<Void> {

    private final String template;
    private final SoyRenderer renderer;

    public UiHeaderRegion(String template, SoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Void input) {
        return renderer.render(template, Collections.emptyMap());
    }

}
