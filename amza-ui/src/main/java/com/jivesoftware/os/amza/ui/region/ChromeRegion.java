package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
// soy.chrome.chromeRegion
public class ChromeRegion<I, R extends PageRegion<I>> implements Region<I> {

    private final String template;
    private final SoyRenderer renderer;
    private final HeaderRegion headerRegion;
    private final List<ManagePlugin> plugins;
    private final R region;

    public ChromeRegion(String template, SoyRenderer renderer, HeaderRegion headerRegion, List<ManagePlugin> plugins, R region) {
        this.template = template;
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.plugins = plugins;
        this.region = region;
    }

    @Override
    public String render(I input) {
        List<Map<String, String>> p = Lists.transform(plugins, (ManagePlugin input1) -> ImmutableMap.of(
            "name", input1.name,
            "path", input1.path,
            "glyphicon", input1.glyphicon)
        );
        Map<String, Object> headerData = new HashMap<>();
        headerData.put("plugins", p);

        Map<String, Object> data = Maps.newHashMap();
        data.put("header", headerRegion.render(headerData));
        data.put("region", region.render(input));
        data.put("title", region.getTitle());
        data.put("plugins", p);
        return renderer.render(template, data);

    }

}
