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
    private final boolean senderEnabled;
    private final boolean receiverEnabled;

    public AmzaAdminRegion(String template, SoyRenderer renderer, boolean senderEnabled, boolean receiverEnabled) {
        this.template = template;
        this.renderer = renderer;
        this.senderEnabled = senderEnabled;
        this.receiverEnabled = receiverEnabled;
    }

    @Override
    public String render(Void input) {
        HashMap<String, Object> data = Maps.newHashMap();
        data.put("sender", senderEnabled);
        data.put("receiver", receiverEnabled);
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Sync";
    }
}
