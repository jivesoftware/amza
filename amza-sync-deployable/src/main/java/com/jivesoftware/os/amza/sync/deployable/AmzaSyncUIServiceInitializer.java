package com.jivesoftware.os.amza.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.sync.deployable.region.AmzaAdminRegion;
import com.jivesoftware.os.amza.sync.deployable.region.AmzaStatusFocusRegion;
import com.jivesoftware.os.amza.sync.deployable.region.AmzaStatusRegion;
import com.jivesoftware.os.amza.ui.region.HeaderRegion;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;

public class AmzaSyncUIServiceInitializer {

    public AmzaSyncUIService initialize(SoyRenderer renderer,
        AmzaSyncSender syncSender,
        ObjectMapper mapper)
        throws Exception {

        return new AmzaSyncUIService(
            renderer,
            new HeaderRegion("soy.amza.chrome.headerRegion", renderer),
            new AmzaAdminRegion("soy.amza.page.adminRegion", renderer),
            new AmzaStatusRegion("soy.amza.page.statusRegion", renderer,
                new AmzaStatusFocusRegion("soy.amza.page.statusFocusRegion", renderer, syncSender, mapper)));
    }
}
