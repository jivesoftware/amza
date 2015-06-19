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
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.EmbeddedAmzaServiceInitializer;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.discovery.AmzaDiscovery;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker;
import com.jivesoftware.os.amza.transport.http.replication.HttpUpdatesTaker;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.ui.AmzaUIInitializer;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.routing.bird.server.InitializeRestfulServer;
import com.jivesoftware.os.routing.bird.server.JerseyEndpoints;
import com.jivesoftware.os.routing.bird.server.RestfulServer;
import java.util.Random;

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
        String logicalName = System.getProperty("amza.logicalName", hostname + ":" + port);

        RingMember ringMember = new RingMember(logicalName);
        RingHost ringHost = new RingHost(hostname, port);

        // todo need a better way to create writter id.
        int writerId = Integer.parseInt(System.getProperty("amza.id", String.valueOf(new Random().nextInt(512))));
        final TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(writerId));

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        final AmzaServiceConfig amzaServiceConfig = new AmzaServiceConfig();
        final AmzaStats amzaStats = new AmzaStats();

        final String[] workingDirs = System.getProperty("amza.working.dirs", "./data1,./data2,./data3")
            .split(",");
        amzaServiceConfig.workingDirectories = workingDirs;

        WALIndexProviderRegistry indexProviderRegistry = new WALIndexProviderRegistry();
        indexProviderRegistry.register("berkeleydb", new BerkeleyDBWALIndexProvider(workingDirs, workingDirs.length));

        UpdatesTaker taker = new HttpUpdatesTaker(amzaStats);

        PartitionPropertyMarshaller partitionPropertyMarshaller = new PartitionPropertyMarshaller() {

            @Override
            public PartitionProperties fromBytes(byte[] bytes) throws Exception {
                return mapper.readValue(bytes, PartitionProperties.class);
            }

            @Override
            public byte[] toBytes(PartitionProperties partitionProperties) throws Exception {
                return mapper.writeValueAsBytes(partitionProperties);
            }
        };

        AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(amzaServiceConfig,
            amzaStats,
            ringMember,
            ringHost,
            orderIdProvider,
            partitionPropertyMarshaller,
            indexProviderRegistry,
            taker,
            Optional.<SendFailureListener>absent(),
            Optional.<TakeFailureListener>absent(),
            (RowsChanged changes) -> {
            });

        final JerseyEndpoints jerseyEndpoints = new JerseyEndpoints()
            .addEndpoint(AmzaEndpoints.class)
            .addInjectable(AmzaService.class, amzaService)
            .addEndpoint(AmzaReplicationRestEndpoints.class)
            .addInjectable(AmzaInstance.class, amzaService);

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
        RestfulServer restfulServer = initializeRestfulServer.build();
        restfulServer.start();

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Jetty Service Online");
        System.out.println("-----------------------------------------------------------------------");

        amzaService.start();

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Amza Service Online");
        System.out.println("-----------------------------------------------------------------------");

        if (clusterName != null) {
            AmzaDiscovery amzaDiscovery = new AmzaDiscovery(amzaService.getAmzaHostRing(), clusterName, multicastGroup, multicastPort);
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
