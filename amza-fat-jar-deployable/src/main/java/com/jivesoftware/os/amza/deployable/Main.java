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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.berkeleydb.BerkeleyDBWALIndexProvider;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexConfig;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexWALIndexProvider;
import com.jivesoftware.os.amza.service.AmzaInstance;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.SickPartitions;
import com.jivesoftware.os.amza.service.discovery.AmzaDiscovery;
import com.jivesoftware.os.amza.service.replication.http.AmzaClientService;
import com.jivesoftware.os.amza.service.replication.http.HttpAvailableRowsTaker;
import com.jivesoftware.os.amza.service.replication.http.HttpRowsTaker;
import com.jivesoftware.os.amza.service.replication.http.endpoints.AmzaClientRestEndpoints;
import com.jivesoftware.os.amza.service.replication.http.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.ui.AmzaUIInitializer;
import com.jivesoftware.os.aquarium.AquariumStats;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.CountersAndTimers;
import com.jivesoftware.os.routing.bird.health.api.HealthTimer;
import com.jivesoftware.os.routing.bird.health.api.NoOpHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
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
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.merlin.config.BindInterfaceToConfiguration;

public class Main {

    public static void main(String[] args) throws Exception {

        try {
            if (args.length == 0) {
                System.out.println("Usage:");
                System.out.println("");
                System.out.println("    java -jar amza.jar <hostName>                        (manual cluster discovery)");
                System.out.println(" or ");
                System.out.println("    java -jar amza.jar <hostName> <clusterName>          (manual cluster discovery)");
                System.out.println(" or ");
                System.out.println("    java -jar amza.jar <hostName> <clusterName> <peers>  (automatic cluster discovery)");
                System.out.println("");
                System.out.println("Overridable properties:");
                System.out.println("");
                System.out.println("    -Damza.logicalName=<logicalName>");
                System.out.println("    -Dhost.datacenter=<datacenterId>");
                System.out.println("    -Dhost.rack=<rackId>");
                System.out.println("");
                System.out.println("    -Damza.port=1175");
                System.out.println("         (change the port used to interact with other nodes.) ");
                System.out.println("");
                System.out.println("    -Damza.id=<writerId>");
                System.out.println("    -Damza.working.dirs=<workingDirs>");
                System.out.println("    -Damza.system.ring.size=<systemRingSize>");
                System.out.println("    -Damza.leap.cache.max.capacity=1000000");
                System.out.println("");
                System.out.println("     Only applicable if you have specified a <clusterName>.");
                System.out.println("          -Damza.discovery.group=225.4.5.6");
                System.out.println("          -Damza.discovery.port=1223");
                System.out.println("");
                System.out.println("Example:");
                System.out.println("java -jar amza.jar " + InetAddress.getLocalHost().getHostName() + " dev");
                System.out.println("");
                System.exit(1);
            } else {
                new Main().run(args);
            }
        } catch (Exception x) {
            x.printStackTrace();
            System.exit(1);
        }
    }

    public void run(String[] args) throws Exception {

        String hostname = args[0];
        String clusterName = (args.length > 1 ? args[1] : "unnamed");
        String hostPortPeers = (args.length > 2 ? args[2] : null);

        int port = Integer.parseInt(System.getProperty("amza.port", "1175"));
        String multicastGroup = System.getProperty("amza.discovery.group", "225.4.5.6");
        int multicastPort = Integer.parseInt(System.getProperty("amza.discovery.port", "1223"));

        String logicalName = System.getProperty("amza.logicalName", hostname + ":" + port);
        String datacenter = System.getProperty("host.datacenter", "unknownDatacenter");
        String rack = System.getProperty("host.rack", "unknownRack");

        RingMember ringMember = new RingMember(logicalName);
        RingHost ringHost = new RingHost(datacenter, rack, hostname, port);

        // todo need a better way to create writer id.
        int writerId = Integer.parseInt(System.getProperty("amza.id", String.valueOf(new Random().nextInt(512))));

        SnowflakeIdPacker idPacker = new SnowflakeIdPacker();
        JiveEpochTimestampProvider timestampProvider = new JiveEpochTimestampProvider();
        final TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(writerId),
            idPacker, timestampProvider);

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        final AmzaServiceConfig amzaServiceConfig = new AmzaServiceConfig();
        final AmzaStats amzaStats = new AmzaStats();
        final SickThreads sickThreads = new SickThreads();
        final SickPartitions sickPartitions = new SickPartitions();

        AtomicInteger systemRingSize = new AtomicInteger(-1);
        amzaServiceConfig.workingDirectories = System.getProperty("amza.working.dirs", "./data1,./data2,./data3")
            .split(",");
        amzaServiceConfig.systemRingSize = Integer.parseInt(System.getProperty("amza.system.ring.size", "-1"));
        if (amzaServiceConfig.systemRingSize > 0) {
            systemRingSize.set(amzaServiceConfig.systemRingSize);
        }

        BAInterner interner = new BAInterner();

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

        //  hmmm
        LABPointerIndexConfig labConfig = BindInterfaceToConfiguration.bindDefault(LABPointerIndexConfig.class);
        labConfig.setLeapCacheMaxCapacity(Integer.parseInt(System.getProperty("amza.leap.cache.max.capacity", "1000000")));

        BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller(); // hehe you cant change this :)
        BinaryHighwaterRowMarshaller highwaterRowMarshaller = new BinaryHighwaterRowMarshaller(interner);

        AtomicReference<Callable<RingTopology>> topologyProvider = new AtomicReference<>(); // bit of a hack

        InstanceDescriptor instanceDescriptor = new InstanceDescriptor(datacenter, rack, "", "", "", "", "", "", "", "", 0, "", "", "", 0L, true);
        ConnectionDescriptorsProvider connectionsProvider = (connectionDescriptorsRequest, expectedReleaseGroup) -> {
            try {
                RingTopology systemRing = topologyProvider.get().call();
                List<ConnectionDescriptor> descriptors = Lists.newArrayList(Iterables.transform(systemRing.entries,
                    input -> new ConnectionDescriptor(instanceDescriptor,
                        false,
                        false,
                        new HostPort(input.ringHost.getHost(), input.ringHost.getPort()),
                        Collections.emptyMap(),
                        Collections.emptyMap())));
                return new ConnectionDescriptorsResponse(200, Collections.emptyList(), "", descriptors, connectionDescriptorsRequest.getRequestUuid());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        TenantsServiceConnectionDescriptorProvider<String> connectionPoolProvider = new TenantsServiceConnectionDescriptorProvider<>(
            Executors.newScheduledThreadPool(1),
            "",
            connectionsProvider,
            "",
            "",
            10_000); // TODO config
        connectionPoolProvider.start();

        TenantAwareHttpClient<String> httpClient = new TenantRoutingHttpClientInitializer<String>(null).builder(
            connectionPoolProvider,
            new HttpDeliveryClientHealthProvider("", null, "", 5000, 100))
            .deadAfterNErrors(10)
            .checkDeadEveryNMillis(10_000)
            .maxConnections(1_000)
            .socketTimeoutInMillis(60_000)
            .build(); //TODO expose to conf

        AvailableRowsTaker availableRowsTaker = new HttpAvailableRowsTaker(httpClient, interner, mapper); // TODO config
        AquariumStats aquariumStats = new AquariumStats();

        AmzaService amzaService = new AmzaServiceInitializer().initialize(amzaServiceConfig,
            interner,
            aquariumStats,
            amzaStats,
            new HealthTimer(CountersAndTimers.getOrCreate("quorumLatency"), "quorumLatency", new NoOpHealthChecker<>("quorumLatency")),
            () -> amzaServiceConfig.systemRingSize,
            sickThreads,
            sickPartitions,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            ringMember,
            ringHost,
            Collections.emptySet(),
            orderIdProvider,
            idPacker,
            partitionPropertyMarshaller,
            (workingIndexDirectories, indexProviderRegistry, ephemeralRowIOProvider, persistentRowIOProvider, partitionStripeFunction) -> {
                indexProviderRegistry.register(
                    new BerkeleyDBWALIndexProvider(BerkeleyDBWALIndexProvider.INDEX_CLASS_NAME, partitionStripeFunction, workingIndexDirectories),
                    persistentRowIOProvider);

                indexProviderRegistry.register(
                    new LABPointerIndexWALIndexProvider(labConfig,
                        LABPointerIndexWALIndexProvider.INDEX_CLASS_NAME,
                        partitionStripeFunction,
                        workingIndexDirectories),
                    persistentRowIOProvider);
            },
            availableRowsTaker,
            () -> new HttpRowsTaker(amzaStats, httpClient, mapper, interner),
            () -> new HttpRowsTaker(amzaStats, httpClient, mapper, interner),
            Optional.absent(),
            (RowsChanged changes) -> {
            });

        topologyProvider.set(() -> amzaService.getRingReader().getRing(AmzaRingReader.SYSTEM_RING, -1));

        AmzaClientProvider<HttpClient, HttpClientException> clientProvider = new AmzaClientProvider<>(
            new HttpPartitionClientFactory(interner),
            new HttpPartitionHostsProvider(interner, httpClient, mapper),
            new RingHostHttpClientProvider(httpClient),
            Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("client-%d").build()),
            10_000, //TODO expose to conf
            -1,
            -1);

        final JerseyEndpoints jerseyEndpoints = new JerseyEndpoints()
            .addEndpoint(AmzaEndpoints.class)
            .addInjectable(AmzaService.class, amzaService)
            .addEndpoint(AmzaReplicationRestEndpoints.class)
            .addInjectable(AmzaInstance.class, amzaService)
            .addEndpoint(AmzaClientRestEndpoints.class)
            .addInjectable(BAInterner.class, interner)
            .addInjectable(AmzaClientService.class, new AmzaClientService(amzaService.getRingReader(), amzaService.getRingWriter(), amzaService));

        new AmzaUIInitializer().initialize(clusterName, ringHost, amzaService, clientProvider, aquariumStats, amzaStats, timestampProvider, idPacker,
            new AmzaUIInitializer.InjectionCallback() {
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

                @Override
                public void addSessionAuth(String... paths) throws Exception {
                    System.out.println("Ignoring session auth request for paths: " + Arrays.toString(paths));
                }
            }
        );

        InitializeRestfulServer initializeRestfulServer = new InitializeRestfulServer(false, port, "AmzaNode", false, null, null, null, 128, 10000);
        initializeRestfulServer.addClasspathResource("/resources");
        initializeRestfulServer.addContextHandler("/", jerseyEndpoints);
        RestfulServer restfulServer = initializeRestfulServer.build();
        restfulServer.start();

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("|      Jetty Service Online");
        System.out.println("-----------------------------------------------------------------------");

        amzaService.start(ringMember, ringHost);

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
                    if (hostPort.length != 2 && hostPort.length != 3) {
                        System.out.println("|     Malformed peer:" + peer + " expected form: <host>:<port> or <logicalName>:<host>:<port>");
                    } else {
                        String peerLogicalName = (hostPort.length == 2) ? hostPort[0] + ":" + hostPort[1] : hostPort[0];
                        String peerHostname = (hostPort.length == 2) ? hostPort[0] : hostPort[1];
                        String peerPort = (hostPort.length == 2) ? hostPort[1] : hostPort[2];

                        RingMember peerRingMember = new RingMember(peerLogicalName);
                        RingHost peerRingHost = new RingHost("unknown", "unknown", peerHostname, Integer.parseInt(peerPort));

                        System.out.println("|     Adding ringMember:" + peerRingMember + " on host:" + peerRingHost + " to cluster: " + clusterName);
                        amzaService.getRingWriter().register(peerRingMember, peerRingHost, writerId, false);
                    }
                }
                systemRingSize.set(1 + peers.length);
                System.out.println("-----------------------------------------------------------------------");
            } else {
                AmzaDiscovery amzaDiscovery = new AmzaDiscovery(amzaService.getRingReader(),
                    amzaService.getRingWriter(),
                    clusterName,
                    multicastGroup,
                    multicastPort,
                    systemRingSize);
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
