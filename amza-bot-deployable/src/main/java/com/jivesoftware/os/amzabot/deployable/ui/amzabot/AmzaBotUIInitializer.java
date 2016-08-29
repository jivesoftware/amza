package com.jivesoftware.os.amzabot.deployable.ui.amzabot;

import com.google.template.soy.SoyFileSet;
import com.google.template.soy.tofu.SoyTofu;
import com.jivesoftware.os.amzabot.deployable.ui.SoyDataUtils;
import com.jivesoftware.os.amzabot.deployable.ui.SoyRenderer;
import com.jivesoftware.os.amzabot.deployable.ui.health.UiChromeRegion;
import com.jivesoftware.os.amzabot.deployable.ui.health.UiHeaderRegion;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.merlin.config.Config;
import org.merlin.config.defaults.StringDefault;

public class AmzaBotUIInitializer {

    public interface AmzaBotUIServiceConfig extends Config {

        @StringDefault("resources/static")
        String getPathToStaticResources();

        @StringDefault("resources/soy")
        String getPathToSoyResources();

    }

    public AmzaBotUIService initialize(String cacheToken,
        AmzaBotUIServiceConfig config) {

        File soyPath = new File(System.getProperty("user.dir"), config.getPathToSoyResources());
        SoyFileSet.Builder soyFileSetBuilder = new SoyFileSet.Builder();

        FileUtils.listFiles(soyPath, null, true).forEach(soyFileSetBuilder::add);

        SoyFileSet sfs = soyFileSetBuilder.build();
        SoyTofu tofu = sfs.compileToTofu();
        SoyRenderer renderer = new SoyRenderer(tofu, new SoyDataUtils());

        AmzaBotRegion contentRegion = new AmzaBotRegion("soy.content.page.main", renderer);

        UiHeaderRegion headerRegion = new UiHeaderRegion("soy.ui.chrome.headerRegion", renderer);
        return new AmzaBotUIService(
            new UiChromeRegion<>(cacheToken, "soy.ui.chrome.chromeRegion", renderer, headerRegion, contentRegion));
    }

}
