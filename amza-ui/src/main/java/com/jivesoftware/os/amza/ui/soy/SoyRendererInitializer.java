package com.jivesoftware.os.amza.ui.soy;

import com.google.template.soy.SoyFileSet;
import com.google.template.soy.tofu.SoyTofu;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.merlin.config.Config;
import org.merlin.config.defaults.StringDefault;

/**
 *
 */
public class SoyRendererInitializer {

    public interface SoyRendererConfig extends Config {

        @StringDefault("resources/static")
        String getPathToStaticResources();

        @StringDefault("resources/soy")
        String getPathToSoyResources();

        void setPathToSoyResources(String pathToSoyResources);
    }

    public SoyRenderer initialize(SoyRendererConfig rendererConfig) {
        File soyPath = new File(System.getProperty("user.dir"), rendererConfig.getPathToSoyResources());
        SoyFileSet.Builder soyFileSetBuilder = new SoyFileSet.Builder();
        for (File file : FileUtils.listFiles(soyPath, null, true)) {
            soyFileSetBuilder.add(file);
        }

        SoyFileSet sfs = soyFileSetBuilder.build();
        SoyTofu tofu = sfs.compileToTofu();
        return new SoyRenderer(tofu, new SoyDataUtils());
    }
}
