/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.deployable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.template.soy.SoyFileSet;
import com.google.template.soy.tofu.SoyTofu;
import com.jivesoftware.os.amza.deployable.ui.AmzaUIEndpoints;
import com.jivesoftware.os.amza.deployable.ui.AmzaUIEndpoints.AmzaClusterName;
import com.jivesoftware.os.amza.deployable.ui.region.AmzaRingPluginRegion;
import com.jivesoftware.os.amza.deployable.ui.region.HeaderRegion;
import com.jivesoftware.os.amza.deployable.ui.region.HealthPluginRegion;
import com.jivesoftware.os.amza.deployable.ui.region.HomeRegion;
import com.jivesoftware.os.amza.deployable.ui.region.ManagePlugin;
import com.jivesoftware.os.amza.deployable.ui.region.endpoints.AmzaRingPluginEndpoints;
import com.jivesoftware.os.amza.deployable.ui.region.endpoints.HealthPluginEndpoints;
import com.jivesoftware.os.amza.deployable.ui.soy.SoyDataUtils;
import com.jivesoftware.os.amza.deployable.ui.soy.SoyRenderer;
import com.jivesoftware.os.amza.deployable.ui.soy.SoyService;
import com.jivesoftware.os.amza.mapdb.MapdbWALIndexProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.EmbeddedAmzaServiceInitializer;
import com.jivesoftware.os.amza.service.discovery.AmzaDiscovery;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.AmzaRing;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALIndexProvider;
import com.jivesoftware.os.amza.transport.http.replication.HttpUpdatesSender;
import com.jivesoftware.os.amza.transport.http.replication.HttpUpdatesTaker;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.transport.tcp.replication.TcpUpdatesSender;
import com.jivesoftware.os.amza.transport.tcp.replication.TcpUpdatesTaker;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.FstMarshaller;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayloadSerializer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.BufferProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.MessageFramer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClientProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpServer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpServerInitializer;
import com.jivesoftware.os.jive.utils.base.service.ServiceHandle;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.server.http.jetty.jersey.server.InitializeRestfulServer;
import com.jivesoftware.os.server.http.jetty.jersey.server.JerseyEndpoints;
import java.util.List;
import java.util.Random;
import org.nustaq.serialization.FSTConfiguration;

public class Main {

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    public void run(String[] args) throws Exception {

        String hostname = args[0];
        int port = Integer.parseInt(System.getProperty("amza.port", "1175"));
        String multicastGroup = System.getProperty("amza.discovery.group", "225.4.5.6");
        int multicastPort = Integer.parseInt(System.getProperty("amza.discovery.port", "1223"));
        String clusterName = (args.length > 1 ? args[1] : null);
        String transport = System.getProperty("amza.transport", "http");

        //TODO pull from properties
        int connectionsPerHost = Integer.parseInt(System.getProperty("amza.tcp.client.connectionsPerHost", "8"));
        int connectTimeoutMillis = Integer.parseInt(System.getProperty("amza.tcp.client.connectTimeoutMillis", "6000000"));
        int socketTimeoutMillis = Integer.parseInt(System.getProperty("amza.tcp.client.socketTimeoutMillis", "6000000"));
        int bufferSize = Integer.parseInt(System.getProperty("amza.tcp.bufferSize", "" + (1024 * 1024 * 10)));
        int numServerThreads = Integer.parseInt(System.getProperty("amza.tcp.server.numThreads", "8"));
        int numBuffers = numServerThreads * 10;
        int tcpPort = Integer.parseInt(System.getProperty("amza.tcp.port", "1177"));

        RingHost ringHost;
        if (transport.equals("http")) {
            ringHost = new RingHost(hostname, port); // TODO include rackId
        } else {
            ringHost = new RingHost(hostname, tcpPort); // TODO include rackId
        }

        // todo need a better way to create writter id.
        final TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(new Random().nextInt(512)));

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        final AmzaServiceConfig amzaServiceConfig = new AmzaServiceConfig();
        final AmzaStats amzaStats = new AmzaStats();

        final String[] rowIndexDirs = new String[]{
            "./rowIndexs/data1",
            "./rowIndexs/data2",
            "./rowIndexs/data3"};

        final WALIndexProvider walIndexProvider = new MapdbWALIndexProvider(rowIndexDirs);

        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());
        marshaller.registerSerializer(MessagePayload.class, new MessagePayloadSerializer());

        IndexReplicationProtocol clientProtocol = new IndexReplicationProtocol(null, orderIdProvider);

        MessageFramer framer = new MessageFramer(marshaller, clientProtocol);
        BufferProvider bufferProvider = new BufferProvider(bufferSize, numBuffers, true, 5);

        TcpClientProvider tcpClientProvider = new TcpClientProvider(
            connectionsPerHost, connectTimeoutMillis, socketTimeoutMillis, bufferSize, bufferSize, bufferProvider, framer);

        UpdatesSender changeSetSender = new TcpUpdatesSender(tcpClientProvider, clientProtocol);
        UpdatesTaker taker = new TcpUpdatesTaker(tcpClientProvider, clientProtocol);

        if (transport.equals("http")) {
            changeSetSender = new HttpUpdatesSender();
            taker = new HttpUpdatesTaker();
        }

        AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(amzaServiceConfig,
            amzaStats,
            ringHost,
            orderIdProvider,
            walIndexProvider,
            changeSetSender,
            taker,
            Optional.<SendFailureListener>absent(),
            Optional.<TakeFailureListener>absent(),
            new RowChanges() {
                @Override
                public void changes(RowsChanged changes) throws Exception {
                }
            });

        amzaService.start();

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Amza Service Online");
        System.out.println("-----------------------------------------------------------------------");

        IndexReplicationProtocol serverProtocol = new IndexReplicationProtocol(amzaService, orderIdProvider);
        bufferProvider = new BufferProvider(bufferSize, numBuffers, true, 5);
        TcpServerInitializer initializer = new TcpServerInitializer();
        // TODO include rackId
        final TcpServer server = initializer.initialize(new RingHost(hostname, tcpPort), numServerThreads, bufferProvider, framer, serverProtocol);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    server.stop();
                } catch (InterruptedException ex) {
                    System.out.println("Failed to stop Tcp Replication Service");
                    ex.printStackTrace(System.out);
                }
            }
        });

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Tcp Replication Service Online");
        System.out.println("-----------------------------------------------------------------------");

        JerseyEndpoints jerseyEndpoints = new JerseyEndpoints()
            .addEndpoint(AmzaEndpoints.class)
            .addInjectable(AmzaService.class, amzaService)
            .addEndpoint(AmzaReplicationRestEndpoints.class)
            .addInjectable(AmzaInstance.class, amzaService);

        SoyFileSet.Builder soyFileSetBuilder = new SoyFileSet.Builder();

        System.out.println("Add....");

        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/chrome.soy"), "chome.soy");
        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/homeRegion.soy"), "home.soy");
        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/healthPluginRegion.soy"), "health.soy");
        soyFileSetBuilder.add(this.getClass().getResource("/resources/soy/amzaRingPluginRegion.soy"), "amzaRing.soy");

        SoyFileSet sfs = soyFileSetBuilder.build();
        SoyTofu tofu = sfs.compileToTofu();
        SoyRenderer renderer = new SoyRenderer(tofu, new SoyDataUtils());
        SoyService soyService = new SoyService(renderer, new HeaderRegion("soy.chrome.headerRegion", renderer),
            new HomeRegion("soy.page.homeRegion", renderer));

        List<ManagePlugin> plugins = Lists.newArrayList(new ManagePlugin("fire", "Health", "/ui/health",
            HealthPluginEndpoints.class,
            new HealthPluginRegion("soy.page.healthPluginRegion", renderer, amzaService.getAmzaRing(), amzaService, amzaStats)),
            new ManagePlugin("leaf", "Amza Ring", "/ui/ring",
                AmzaRingPluginEndpoints.class,
                new AmzaRingPluginRegion("soy.page.amzaRingPluginRegion", renderer, amzaService.getAmzaRing())));

        jerseyEndpoints.addInjectable(SoyService.class, soyService);

        for (ManagePlugin plugin : plugins) {
            soyService.registerPlugin(plugin);
            jerseyEndpoints.addEndpoint(plugin.endpointsClass);
            jerseyEndpoints.addInjectable(plugin.region.getClass(), plugin.region);
        }

        jerseyEndpoints.addEndpoint(AmzaUIEndpoints.class);
        jerseyEndpoints.addInjectable(AmzaClusterName.class, new AmzaClusterName((clusterName == null) ? "manual" : clusterName));
        jerseyEndpoints.addInjectable(AmzaRing.class, amzaService.getAmzaRing());
        jerseyEndpoints.addInjectable(AmzaStats.class, amzaStats);
        jerseyEndpoints.addInjectable(RingHost.class, ringHost);

        InitializeRestfulServer initializeRestfulServer = new InitializeRestfulServer(port, "AmzaNode", 128, 10000);
        initializeRestfulServer.addClasspathResource("/resources");
        initializeRestfulServer.addContextHandler("/", jerseyEndpoints);
        ServiceHandle serviceHandle = initializeRestfulServer.build();
        serviceHandle.start();

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Jetty Service Online");
        System.out.println("-----------------------------------------------------------------------");

        if (clusterName != null) {
            AmzaDiscovery amzaDiscovery = new AmzaDiscovery(amzaService.getAmzaRing(), ringHost, clusterName, multicastGroup, multicastPort);
            amzaDiscovery.start();
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|      Amza Service Discovery Online");
            System.out.println("-----------------------------------------------------------------------");
        } else {
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|     Amze Service is in manual Discovery mode.  No cluster name was specified");
            System.out.println("-----------------------------------------------------------------------");
        }
    }
}
