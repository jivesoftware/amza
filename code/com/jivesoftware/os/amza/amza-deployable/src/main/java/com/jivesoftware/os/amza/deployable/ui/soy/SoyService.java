package com.jivesoftware.os.amza.deployable.ui.soy;

import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.deployable.ui.region.ChromeRegion;
import com.jivesoftware.os.amza.deployable.ui.region.HeaderRegion;
import com.jivesoftware.os.amza.deployable.ui.region.HomeRegion.HomeInput;
import com.jivesoftware.os.amza.deployable.ui.region.ManagePlugin;
import com.jivesoftware.os.amza.deployable.ui.region.PageRegion;
import java.util.List;

/**
 *
 */
public class SoyService {

    private final SoyRenderer renderer;
    private final HeaderRegion headerRegion;
    private final PageRegion<HomeInput> homeRegion;

    private final List<ManagePlugin> plugins = Lists.newCopyOnWriteArrayList();

    public SoyService(
        SoyRenderer renderer,
        HeaderRegion headerRegion,
        PageRegion<HomeInput> homeRegion
    ) {
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.homeRegion = homeRegion;

    }

    public String render(String amzaJarWGetURL, String amzaClusterName) {

        return chrome(homeRegion).render(new HomeInput(amzaJarWGetURL, amzaClusterName));
    }

    public void registerPlugin(ManagePlugin plugin) {
        plugins.add(plugin);
    }

    private <I, R extends PageRegion<I>> ChromeRegion<I, R> chrome(R region) {
        return new ChromeRegion<>("soy.chrome.chromeRegion", renderer, headerRegion, plugins, region);
    }

    public <I> String renderPlugin(PageRegion<I> pluginRegion, I input) {
        return chrome(pluginRegion).render(input);
    }
}
