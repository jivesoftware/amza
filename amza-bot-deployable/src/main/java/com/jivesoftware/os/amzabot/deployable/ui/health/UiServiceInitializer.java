package com.jivesoftware.os.amzabot.deployable.ui.health;

import com.google.template.soy.SoyFileSet;
import com.google.template.soy.tofu.SoyTofu;
import com.jivesoftware.os.amzabot.deployable.ui.SoyDataUtils;
import com.jivesoftware.os.amzabot.deployable.ui.SoyRenderer;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.merlin.config.Config;
import org.merlin.config.defaults.StringDefault;

public class UiServiceInitializer {

    public interface UiServiceConfig extends Config {
        @StringDefault("resources/soy")
        String getPathToSoyResources();
    }

    public UiService initialize(String cacheToken, UiServiceConfig config) {
        File soyPath = new File(System.getProperty("user.dir"), config.getPathToSoyResources());
        SoyFileSet.Builder soyFileSetBuilder = new SoyFileSet.Builder();

        FileUtils.listFiles(soyPath, null, true).forEach(file -> soyFileSetBuilder.add(file));
        SoyFileSet sfs = soyFileSetBuilder.build();
        SoyTofu tofu = sfs.compileToTofu();
        SoyRenderer renderer = new SoyRenderer(tofu, new SoyDataUtils());

        return new UiService(
            new UiChromeRegion<>(cacheToken, "soy.ui.chrome.chromeRegion", renderer,
                new UiHeaderRegion("soy.ui.chrome.headerRegion", renderer),
                new UiHomeRegion("soy.ui.page.homeRegion", renderer)));
    }

}
