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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.berkeleydb.BerkeleyDBWALIndexProvider;
import com.jivesoftware.os.amza.client.http.AmzaHttpClientProvider;
import com.jivesoftware.os.amza.client.http.PartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.amza.lsm.pointers.LSMPointerIndexWALIndexProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.EmbeddedAmzaServiceInitializer;
import com.jivesoftware.os.amza.service.discovery.AmzaDiscovery;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.PartitionProvider;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.RingTopology;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.transport.http.replication.HttpAvailableRowsTaker;
import com.jivesoftware.os.amza.transport.http.replication.HttpRowsTaker;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaClientRestEndpoints;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.ui.AmzaUIInitializer;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.InitializeRestfulServer;
import com.jivesoftware.os.routing.bird.server.JerseyEndpoints;
import com.jivesoftware.os.routing.bird.server.RestfulServer;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptorsProvider;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptorsResponse;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.InstanceDescriptor;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;

public class Main {

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    public void run(String[] args) throws Exception {

        String hostname = args[0];
        int port = Integer.parseInt(System.getProperty("amza.port", "1175"));
        String multicastGroup = System.getProperty("amza.discovery.group", "225.4.5.6");
        int multicastPort = Integer.parseInt(System.getProperty("amza.discovery.port", "1223"));
        String clusterName = (args.length > 1 ? args[1] : "unnamed");
        String hostPortPeers = (args.length > 2 ? args[2] : null);

        String logicalName = System.getProperty("amza.logicalName", hostname + ":" + port);

        RingMember ringMember = new RingMember(logicalName);
        RingHost ringHost = new RingHost(hostname, port);

        // todo need a better way to create writter id.
        int writerId = Integer.parseInt(System.getProperty("amza.id", String.valueOf(new Random().nextInt(512))));
        SnowflakeIdPacker idPacker = new SnowflakeIdPacker();
        final TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(writerId),
            idPacker,
            new JiveEpochTimestampProvider());

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        final AmzaServiceConfig amzaServiceConfig = new AmzaServiceConfig();
        final AmzaStats amzaStats = new AmzaStats();

        final String[] workingDirs = System.getProperty("amza.working.dirs", "./data1,./data2,./data3")
            .split(",");
        amzaServiceConfig.workingDirectories = workingDirs;

        AvailableRowsTaker availableRowsTaker = new HttpAvailableRowsTaker(amzaStats);

        PartitionPropertyMarshaller partitionPropertyMarshaller = new PartitionPropertyMarshaller() {

            @Override
            public PartitionProperties fromBytes(byte[] bytes) {
                try {
                    return mapper.readValue(bytes, PartitionProperties.class);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }

            @Override
            public byte[] toBytes(PartitionProperties partitionProperties) {
                try {
                    return mapper.writeValueAsBytes(partitionProperties);
                } catch (JsonProcessingException ex) {
                    throw new RuntimeException(ex);
                }
            }
        };

        AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(amzaServiceConfig,
            amzaStats,
            ringMember,
            ringHost,
            orderIdProvider,
            idPacker,
            partitionPropertyMarshaller,
            (indexProviderRegistry, ephemeralRowIOProvider, persistentRowIOProvider) -> {
                indexProviderRegistry.register(BerkeleyDBWALIndexProvider.INDEX_CLASS_NAME,
                    new BerkeleyDBWALIndexProvider(workingDirs, workingDirs.length), persistentRowIOProvider);

                indexProviderRegistry.register(LSMPointerIndexWALIndexProvider.INDEX_CLASS_NAME,
                    new LSMPointerIndexWALIndexProvider(workingDirs, workingDirs.length), persistentRowIOProvider);
            },
            availableRowsTaker,
            () -> new HttpRowsTaker(amzaStats),
            Optional.<TakeFailureListener>absent(),
            (RowsChanged changes) -> {
            });

        InstanceDescriptor instanceDescriptor = new InstanceDescriptor("", "", "", "", "", "", "", "", 0, "", "", 0L, true);
        ConnectionDescriptorsProvider connectionsProvider = connectionDescriptorsRequest -> {
            try {
                RingTopology systemRing = amzaService.getRingReader().getRing(AmzaRingReader.SYSTEM_RING);
                List<ConnectionDescriptor> descriptors = Lists.newArrayList(Iterables.transform(systemRing.entries,
                    input -> new ConnectionDescriptor(instanceDescriptor,
                        new HostPort(input.ringHost.getHost(), input.ringHost.getPort()),
                        Collections.emptyMap())));
                return new ConnectionDescriptorsResponse(200, Collections.emptyList(), "", descriptors);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        TenantAwareHttpClient<String> httpClient = new TenantRoutingHttpClientInitializer<String>().initialize(
            new TenantsServiceConnectionDescriptorProvider<>("",
                connectionsProvider,
                "",
                ""),
            new HttpDeliveryClientHealthProvider("", null, "", 5000, 100),
            10,
            10_000); //TODO expose to conf

        AmzaHttpClientProvider clientProvider = new AmzaHttpClientProvider(
            new PartitionHostsProvider(httpClient),
            new RingHostHttpClientProvider(httpClient),
            Executors.newCachedThreadPool(),
            10_000); //TODO expose to conf

        final JerseyEndpoints jerseyEndpoints = new JerseyEndpoints()
            .addEndpoint(AmzaEndpoints.class)
            .addInjectable(AmzaService.class, amzaService)
            .addEndpoint(AmzaReplicationRestEndpoints.class)
            .addInjectable(AmzaInstance.class, amzaService)
            .addInjectable(AmzaStats.class, amzaStats)
            .addEndpoint(AmzaClientRestEndpoints.class)
            .addInjectable(AmzaRingReader.class, amzaService.getRingReader())
            .addInjectable(PartitionProvider.class, amzaService);

        new AmzaUIInitializer().initialize(clusterName, ringHost, amzaService, clientProvider, amzaStats, new AmzaUIInitializer.InjectionCallback() {

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
            if (hostPortPeers != null) {
                System.out.println("-----------------------------------------------------------------------");
                System.out.println("|     Amza Service is in manual Discovery mode. Cluster Name:" + clusterName);
                String[] peers = hostPortPeers.split(",");
                for (String peer : peers) {
                    String[] hostPort = peer.trim().split(":");
                    if (hostPort.length > 2) {
                        System.out.println("|     Malformed peer:" + peer + " expected form: <host>:<port> or <logicalName>:<host>:<port>");
                    } else {
                        String peerLogicalName = (hostPort.length == 2) ? hostPort[0] + ":" + hostPort[1] : hostPort[0];
                        String peerHostname = (hostPort.length == 2) ? hostPort[0] : hostPort[1];
                        String peerPort = (hostPort.length == 2) ? hostPort[1] : hostPort[2];

                        RingMember peerRingMember = new RingMember(peerLogicalName);
                        RingHost peerRingHost = new RingHost(peerHostname, Integer.parseInt(peerPort));

                        System.out.println("|     Adding ringMember:" + peerRingMember + " on host:" + peerRingHost + " to cluster: " + clusterName);
                        amzaService.getRingWriter().register(peerRingMember, peerRingHost, writerId);
                    }
                }
                System.out.println("-----------------------------------------------------------------------");
            } else {
                AmzaDiscovery amzaDiscovery = new AmzaDiscovery(amzaService.getRingReader(),
                    amzaService.getRingWriter(),
                    clusterName,
                    multicastGroup,
                    multicastPort);
                amzaDiscovery.start();
                System.out.println("-----------------------------------------------------------------------");
                System.out.println("|      Amza Service Discovery Online: Cluster Name:" + clusterName);
                System.out.println("-----------------------------------------------------------------------");
            }
        } else {
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|     Amza Service is in manual Discovery mode.  No cluster name was specified");
            System.out.println("-----------------------------------------------------------------------");
        }
    }
}
