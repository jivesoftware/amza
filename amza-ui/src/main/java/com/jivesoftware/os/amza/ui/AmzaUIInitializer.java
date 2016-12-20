package com.jivesoftware.os.amza.ui;

import com.google.common.collect.Lists;
import com.google.template.soy.SoyFileSet;
import com.google.template.soy.tofu.SoyTofu;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.ring.AmzaRingWriter;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.ui.endpoints.AmzaChatterPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.AmzaClusterPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.AmzaInspectPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.AmzaPartitionsPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.AmzaRingsPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.AmzaStressPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.AmzaUIEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.AmzaUIEndpoints.AmzaClusterName;
import com.jivesoftware.os.amza.ui.endpoints.AquariumPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.CompactionsPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.MetricsPluginEndpoints;
import com.jivesoftware.os.amza.ui.endpoints.OverviewPluginEndpoints;
import com.jivesoftware.os.amza.ui.region.AmzaChatterPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaClusterPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaInspectPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaPartitionsPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaRingsPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaStressPluginRegion;
import com.jivesoftware.os.amza.ui.region.AquariumPluginRegion;
import com.jivesoftware.os.amza.ui.region.CompactionsPluginRegion;
import com.jivesoftware.os.amza.ui.region.HeaderRegion;
import com.jivesoftware.os.amza.ui.region.HomeRegion;
import com.jivesoftware.os.amza.ui.region.ManagePlugin;
import com.jivesoftware.os.amza.ui.region.MetricsPluginRegion;
import com.jivesoftware.os.amza.ui.region.OverviewRegion;
import com.jivesoftware.os.amza.ui.soy.SoyDataUtils;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.amza.ui.soy.SoyService;
import com.jivesoftware.os.aquarium.AquariumStats;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.io.IOUtils;

/**
 * @author jonathan.colt
 */
public class AmzaUIInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public interface InjectionCallback {

        void addEndpoint(Class clazz);

        void addInjectable(Class clazz, Object instance);

        void addSessionAuth(String... paths) throws Exception;
    }

    public void initialize(String clusterName,
        RingHost host,
        AmzaService amzaService,
        PartitionClientProvider clientProvider,
        AquariumStats aquariumStats,
        AmzaStats amzaStats,
        TimestampProvider timestampProvider,
        IdPacker idPacker,
        InjectionCallback injectionCallback) throws Exception {

        SoyFileSet.Builder soyFileSetBuilder = new SoyFileSet.Builder();

        URL dirURL = AmzaUIInitializer.class.getClassLoader().getResource("resources/soy/amza/");
        if (dirURL != null && dirURL.getProtocol().equals("jar")) {
            String jarPath = dirURL.getPath().substring(5, dirURL.getPath().indexOf("!"));
            JarFile jar = new JarFile(URLDecoder.decode(jarPath, "UTF-8"));
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                String name = entries.nextElement().getName();
                if (name.endsWith(".soy") && name.startsWith("resources/soy/amza/")) {
                    String soyName = name.substring(name.lastIndexOf('/') + 1);
                    LOG.info("/" + name + " " + soyName);
                    soyFileSetBuilder.add(this.getClass().getResource("/" + name), soyName);
                }
            }
        } else {
            List<String> soyFiles = IOUtils.readLines(this.getClass().getResourceAsStream("resources/soy/amza/"), StandardCharsets.UTF_8);
            for (String soyFile : soyFiles) {
                LOG.info("Adding {}", soyFile);
                soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/amza/" + soyFile), soyFile);
            }
        }

        SoyFileSet sfs = soyFileSetBuilder.build();
        SoyTofu tofu = sfs.compileToTofu();
        SoyRenderer renderer = new SoyRenderer(tofu, new SoyDataUtils());
        SoyService soyService = new SoyService(renderer, new HeaderRegion("soy.chrome.headerRegion", renderer),
            new HomeRegion("soy.page.homeRegion", renderer));

        ManagePlugin overviewPlugin = new ManagePlugin("dashboard", "Overview", "/amza/ui/overview",
            OverviewPluginEndpoints.class,
            new OverviewRegion("soy.page.overviewRegion", renderer));

        ManagePlugin inspectPlugin = new ManagePlugin("search", "Inspect", "/amza/ui/inspect",
            AmzaInspectPluginEndpoints.class,
            new AmzaInspectPluginRegion("soy.page.amzaInspectPluginRegion", renderer, amzaService, clientProvider));

        ManagePlugin metricsPlugin = new ManagePlugin("dashboard", "Metrics", "/amza/ui/metrics",
            MetricsPluginEndpoints.class,
            new MetricsPluginRegion("soy.page.metricsPluginRegion", "soy.page.partitionMetricsPluginRegion",
                "soy.page.amzaStats", "soy.page.visualizePartitionPluginRegion", renderer, amzaService.getRingReader(), amzaService, amzaStats,
                timestampProvider, idPacker));

        ManagePlugin compactionsPlugin = new ManagePlugin("compressed", "Compactions", "/amza/ui/compactions",
            CompactionsPluginEndpoints.class,
            new CompactionsPluginRegion("soy.page.compactionsPluginRegion", renderer, amzaService.getRingReader(), amzaService, amzaStats));

        ManagePlugin ringsPlugin = new ManagePlugin("repeat", "Rings", "/amza/ui/rings",
            AmzaRingsPluginEndpoints.class,
            new AmzaRingsPluginRegion("soy.page.amzaRingsPluginRegion", renderer, amzaService.getRingWriter(), amzaService.getRingReader()));

        ManagePlugin partitionsPlugin = new ManagePlugin("map-marker", "Partitions", "/amza/ui/partitions",
            AmzaPartitionsPluginEndpoints.class,
            new AmzaPartitionsPluginRegion("soy.page.amzaPartitionsPluginRegion", renderer, amzaService.getRingReader(), amzaService));

        ManagePlugin aquariumPlugin = new ManagePlugin("piggy-bank", "Aquarium", "/amza/ui/aquarium",
            AquariumPluginEndpoints.class,
            new AquariumPluginRegion("soy.page.aquariumPluginRegion", renderer, amzaService.getRingReader(),
                amzaService.getAquariumProvider(), amzaService.getLiveliness(), aquariumStats));

        ManagePlugin clusterPlugin = new ManagePlugin("leaf", "Cluster", "/amza/ui/cluster",
            AmzaClusterPluginEndpoints.class,
            new AmzaClusterPluginRegion("soy.page.amzaClusterPluginRegion", renderer, amzaService.getRingWriter(), amzaService.getRingReader()));

        ManagePlugin stressPlugin = new ManagePlugin("scale", "Stress", "/amza/ui/stress",
            AmzaStressPluginEndpoints.class,
            new AmzaStressPluginRegion("soy.page.amzaStressPluginRegion", renderer, amzaService, clientProvider));

        ManagePlugin chatterPlugin = new ManagePlugin("transfer", "Chatter", "/amza/ui/chatter",
            AmzaChatterPluginEndpoints.class,
            new AmzaChatterPluginRegion("soy.page.amzaChatterPluginRegion", renderer, amzaService, amzaStats, timestampProvider, idPacker));

        List<ManagePlugin> plugins = Lists.newArrayList(overviewPlugin,
            aquariumPlugin,
            ringsPlugin,
            partitionsPlugin,
            chatterPlugin,
            metricsPlugin,
            inspectPlugin,
            compactionsPlugin,
            clusterPlugin,
            stressPlugin);

        injectionCallback.addInjectable(SoyService.class, soyService);

        for (ManagePlugin plugin : plugins) {
            soyService.registerPlugin(plugin);
            injectionCallback.addEndpoint(plugin.endpointsClass);
            injectionCallback.addInjectable(plugin.region.getClass(), plugin.region);
        }

        injectionCallback.addSessionAuth("/amza/*");

        injectionCallback.addEndpoint(AmzaUIEndpoints.class);
        injectionCallback.addInjectable(AmzaClusterName.class, new AmzaClusterName((clusterName == null) ? "manual" : clusterName));
        injectionCallback.addInjectable(AmzaRingWriter.class, amzaService.getRingWriter());
        injectionCallback.addInjectable(AmzaStats.class, amzaStats);
        injectionCallback.addInjectable(AquariumStats.class, aquariumStats);
        injectionCallback.addInjectable(RingHost.class, host);
    }
}
