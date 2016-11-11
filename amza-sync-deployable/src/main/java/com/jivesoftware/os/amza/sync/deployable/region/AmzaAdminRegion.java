package com.jivesoftware.os.amza.sync.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.ui.region.PageRegion;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import java.util.HashMap;

/**
 *
 */
public class AmzaAdminRegion implements PageRegion<Void> {

    private final String template;
    private final SoyRenderer renderer;

    public AmzaAdminRegion(String template, SoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Void input) {
        HashMap<String, Object> data = Maps.newHashMap();
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Sync";
    }
}
