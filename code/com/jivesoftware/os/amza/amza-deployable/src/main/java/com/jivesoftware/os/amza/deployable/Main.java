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
import com.jivesoftware.os.amza.berkeleydb.BerkeleyDBWALIndexProvider;
import com.jivesoftware.os.amza.mapdb.MapdbWALIndexProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.EmbeddedAmzaServiceInitializer;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.discovery.AmzaDiscovery;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.RegionPropertyMarshaller;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.HighwaterMarks;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
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
import com.jivesoftware.os.amza.ui.AmzaUIInitializer;
import com.jivesoftware.os.jive.utils.base.service.ServiceHandle;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.server.http.jetty.jersey.server.InitializeRestfulServer;
import com.jivesoftware.os.server.http.jetty.jersey.server.JerseyEndpoints;
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
            ringHost = new RingHost(hostname, port);
        } else {
            ringHost = new RingHost(hostname, tcpPort);
        }

        // todo need a better way to create writter id.
        int writerId = Integer.parseInt(System.getProperty("amza.id", String.valueOf(new Random().nextInt(512))));
        final TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(writerId));

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        final AmzaServiceConfig amzaServiceConfig = new AmzaServiceConfig();
        final AmzaStats amzaStats = new AmzaStats();

        final String[] mapdbRowIndexDirs = new String[]{
            "./mapdb/data1",
            "./mapdb/data2",
            "./mapdb/data3"};

        final String[] berkeleydbRowIndexDirs = new String[]{
            "./berkeleydb/data1",
            "./berkeleydb/data2",
            "./berkeleydb/data3"};

        WALIndexProviderRegistry indexProviderRegistry = new WALIndexProviderRegistry();
        indexProviderRegistry.register("mapdb", new MapdbWALIndexProvider(mapdbRowIndexDirs));
        indexProviderRegistry.register("berkeleydb", new BerkeleyDBWALIndexProvider(berkeleydbRowIndexDirs));

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
            changeSetSender = new HttpUpdatesSender(amzaStats);
            taker = new HttpUpdatesTaker(amzaStats);
        }

        RegionPropertyMarshaller regionPropertyMarshaller = new RegionPropertyMarshaller() {

            @Override
            public RegionProperties fromBytes(byte[] bytes) throws Exception {
                return mapper.readValue(bytes, RegionProperties.class);
            }

            @Override
            public byte[] toBytes(RegionProperties regionProperties) throws Exception {
                return mapper.writeValueAsBytes(regionProperties);
            }
        };


        AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(amzaServiceConfig,
            amzaStats,
            ringHost,
            orderIdProvider,
            regionPropertyMarshaller,
            indexProviderRegistry,
            changeSetSender,
            taker,
            Optional.<SendFailureListener>absent(),
            Optional.<TakeFailureListener>absent(),
            new RowChanges() {
                @Override
                public void changes(RowsChanged changes) throws Exception {
                }
            });

        IndexReplicationProtocol serverProtocol = new IndexReplicationProtocol(amzaService, orderIdProvider);
        bufferProvider = new BufferProvider(bufferSize, numBuffers, true, 5);
        TcpServerInitializer initializer = new TcpServerInitializer();
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

        final JerseyEndpoints jerseyEndpoints = new JerseyEndpoints()
            .addEndpoint(AmzaEndpoints.class)
            .addInjectable(AmzaService.class, amzaService)
            .addEndpoint(AmzaReplicationRestEndpoints.class)
            .addInjectable(AmzaInstance.class, amzaService)
            .addInjectable(HighwaterMarks.class, amzaService.getHighwaterMarks());

        new AmzaUIInitializer().initialize(clusterName, ringHost, amzaService, amzaStats, new AmzaUIInitializer.InjectionCallback() {

            @Override
            public void addEndpoint(Class clazz) {
                System.out.println("Adding endpoint=" + clazz);
                jerseyEndpoints.addEndpoint(clazz);
            }

            @Override
            public void addInjectable(Class clazz, Object instance) {
                System.out.println("Injecting " + clazz + " " + instance);
                jerseyEndpoints.addInjectable(clazz, instance);
            }
        });

        InitializeRestfulServer initializeRestfulServer = new InitializeRestfulServer(port, "AmzaNode", 128, 10000);
        initializeRestfulServer.addClasspathResource("/resources");
        initializeRestfulServer.addContextHandler("/", jerseyEndpoints);
        ServiceHandle serviceHandle = initializeRestfulServer.build();
        serviceHandle.start();

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Jetty Service Online");
        System.out.println("-----------------------------------------------------------------------");

        amzaService.start();

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Amza Service Online");
        System.out.println("-----------------------------------------------------------------------");

        if (clusterName != null) {
            AmzaDiscovery amzaDiscovery = new AmzaDiscovery(amzaService.getAmzaRing(), ringHost, clusterName, multicastGroup, multicastPort);
            amzaDiscovery.start();
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|      Amza Service Discovery Online");
            System.out.println("-----------------------------------------------------------------------");
        } else {
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|     Amza Service is in manual Discovery mode.  No cluster name was specified");
            System.out.println("-----------------------------------------------------------------------");
        }
    }
}
