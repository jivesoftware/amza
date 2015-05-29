package com.jivesoftware.os.amza.ui;

import com.google.common.collect.Lists;
import com.google.template.soy.SoyFileSet;
import com.google.template.soy.tofu.SoyTofu;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.AmzaRing;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.ui.endpoints.AmzaClusterPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.AmzaInspectPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.AmzaRegionsPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.AmzaRingsPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.AmzaStressPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.AmzaUIEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.AmzaUIEndpoints.AmzaClusterName;
import com.jivesoftware.os.amza.ui.endpoints.HealthPluginEndpoints;
import com.jivesoftware.os.amza.ui.region.AmzaClusterPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaInspectPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaRegionsPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaRingsPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaStressPluginRegion;
import com.jivesoftware.os.amza.ui.region.HeaderRegion;
import com.jivesoftware.os.amza.ui.region.HealthPluginRegion;
import com.jivesoftware.os.amza.ui.region.HomeRegion;
import com.jivesoftware.os.amza.ui.region.ManagePlugin;
import com.jivesoftware.os.amza.ui.soy.SoyDataUtils;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.amza.ui.soy.SoyService;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class AmzaUIInitializer {

    public static interface InjectionCallback {

        void addEndpoint(Class clazz);

        void addInjectable(Class clazz, Object instance);
    }

    public void initialize(String clusterName, RingHost host, AmzaService amzaService, AmzaStats amzaStats, InjectionCallback injectionCallback) {

        SoyFileSet.Builder soyFileSetBuilder = new SoyFileSet.Builder();

        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/amza/chrome.soy"), "chome.soy");
        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/amza/homeRegion.soy"), "home.soy");
        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/amza/healthPluginRegion.soy"), "health.soy");
        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/amza/amzaRingsPluginRegion.soy"), "amzaRings.soy");
        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/amza/amzaClusterPluginRegion.soy"), "amzaCluster.soy");
        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/amza/amzaRegionsPluginRegion.soy"), "amzaRegions.soy");
        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/amza/amzaStats.soy"), "amzaStats.soy");
        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/amza/amzaStackedProgress.soy"), "amzaStackedProgress.soy");
        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/amza/amzaStressPluginRegion.soy"), "amzaStress.soy");
        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/amza/amzaInspectPluginRegion.soy"), "amzaInspect.soy");

        SoyFileSet sfs = soyFileSetBuilder.build();
        SoyTofu tofu = sfs.compileToTofu();
        SoyRenderer renderer = new SoyRenderer(tofu, new SoyDataUtils());
        SoyService soyService = new SoyService(renderer, new HeaderRegion("soy.chrome.headerRegion", renderer),
            new HomeRegion("soy.page.homeRegion", renderer));

        List<ManagePlugin> plugins = Lists.newArrayList(
            new ManagePlugin("dashboard", "Metrics", "/amza/ui/metrics",
                HealthPluginEndpoints.class,
                new HealthPluginRegion("soy.page.healthPluginRegion", "soy.page.amzaStats", renderer, amzaService.getAmzaHostRing(), amzaService, amzaStats)),
            new ManagePlugin("repeat", "Rings", "/amza/ui/rings",
                AmzaRingsPluginEndpoints.class,
                new AmzaRingsPluginRegion("soy.page.amzaRingsPluginRegion", renderer, amzaService.getAmzaHostRing())),
            new ManagePlugin("map-marker", "Regions", "/amza/ui/regions",
                AmzaRegionsPluginEndpoints.class,
                new AmzaRegionsPluginRegion("soy.page.amzaRegionsPluginRegion", renderer, amzaService.getAmzaHostRing(), amzaService)),
            new ManagePlugin("leaf", "Cluster", "/amza/ui/cluster",
                AmzaClusterPluginEndpoints.class,
                new AmzaClusterPluginRegion("soy.page.amzaClusterPluginRegion", renderer, amzaService.getAmzaHostRing())),
            new ManagePlugin("scale", "Stress", "/amza/ui/stress",
                AmzaStressPluginEndpoints.class,
                new AmzaStressPluginRegion("soy.page.amzaStressPluginRegion", renderer, amzaService)),
            new ManagePlugin("search", "Inspect", "/amza/ui/inspect",
                AmzaInspectPluginEndpoints.class,
                new AmzaInspectPluginRegion("soy.page.amzaInspectPluginRegion", renderer, amzaService)));

        injectionCallback.addInjectable(SoyService.class, soyService);

        for (ManagePlugin plugin : plugins) {
            soyService.registerPlugin(plugin);
            injectionCallback.addEndpoint(plugin.endpointsClass);
            injectionCallback.addInjectable(plugin.region.getClass(), plugin.region);
        }

        injectionCallback.addEndpoint(AmzaUIEndpoints.class);
        injectionCallback.addInjectable(AmzaClusterName.class, new AmzaClusterName((clusterName == null) ? "manual" : clusterName));
        injectionCallback.addInjectable(AmzaRing.class, amzaService.getAmzaHostRing());
        injectionCallback.addInjectable(AmzaStats.class, amzaStats);
        injectionCallback.addInjectable(RingHost.class, host);
    }
}
