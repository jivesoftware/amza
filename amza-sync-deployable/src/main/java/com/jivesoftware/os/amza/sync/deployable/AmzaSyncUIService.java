package com.jivesoftware.os.amza.sync.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.ui.region.ChromeRegion;
import com.jivesoftware.os.amza.ui.region.HeaderRegion;
import com.jivesoftware.os.amza.ui.region.ManagePlugin;
import com.jivesoftware.os.amza.ui.region.PageRegion;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import java.util.List;

/**
 *
 */
public class AmzaSyncUIService {

    private final SoyRenderer renderer;
    private final HeaderRegion headerRegion;
    private final PageRegion<Void> adminRegion;
    private final PageRegion<PartitionName> statusRegion;

    private final List<ManagePlugin> plugins = Lists.newCopyOnWriteArrayList();

    public AmzaSyncUIService(
        SoyRenderer renderer,
        HeaderRegion headerRegion,
        PageRegion<Void> adminRegion,
        PageRegion<PartitionName> statusRegion) {
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.adminRegion = adminRegion;
        this.statusRegion = statusRegion;
    }

    private <I, R extends PageRegion<I>> ChromeRegion<I, R> chrome(R region) {
        return new ChromeRegion<>("soy.amza.chrome.chromeRegion", renderer, headerRegion, plugins, region);
    }

    public String render() {
        return chrome(adminRegion).render(null);
    }

    public String renderStatus(PartitionName partitionName) {
        return chrome(statusRegion).render(partitionName);
    }
}
