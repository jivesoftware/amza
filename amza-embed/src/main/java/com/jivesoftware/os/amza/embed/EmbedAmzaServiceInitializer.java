package com.jivesoftware.os.amza.embed;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowChanges;
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
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.replication.http.AmzaClientService;
import com.jivesoftware.os.amza.service.replication.http.AmzaRestClient;
import com.jivesoftware.os.amza.service.replication.http.HttpAvailableRowsTaker;
import com.jivesoftware.os.amza.service.replication.http.HttpRowsTaker;
import com.jivesoftware.os.amza.service.replication.http.endpoints.AmzaClientRestEndpoints;
import com.jivesoftware.os.amza.service.replication.http.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.AmzaRingWriter;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryPrimaryRowMarshaller;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.service.take.RowsTakerFactory;
import com.jivesoftware.os.amza.ui.AmzaUIInitializer;
import com.jivesoftware.os.aquarium.AquariumStats;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.health.api.SickHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreadsHealthCheck;
import com.jivesoftware.os.routing.bird.http.client.ClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executors;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public class EmbedAmzaServiceInitializer {

    interface AmzaSickThreadsHealthConfig extends SickHealthCheckConfig {

        @Override
        @StringDefault("sick>threads")
        String getName();

        @Override
        @StringDefault("No sick threads")
        String getDescription();

        @Override
        @DoubleDefault(0.2)
        Double getSickHealth();
    }

    public Lifecycle initialize(Deployable deployable,
        ClientHealthProvider clientHealthProvider,
        int instanceId,
        String instanceKey,
        String serviceName,
        String datacenterName,
        String rackName,
        String hostName,
        int port,
        String clusterName,
        AmzaServiceConfig amzaServiceConfig,
        LABPointerIndexConfig indexConfig,
        AmzaStats amzaStats,
        BAInterner baInterner,
        SnowflakeIdPacker idPacker,
        JiveEpochTimestampProvider timestampProvider,
        Set<RingMember> blacklistRingMembers,
        boolean useAmzaDiscovery,
        boolean bindClientEndpoints,
        RowChanges allRowChanges) throws Exception {

        SickThreads sickThreads = new SickThreads();
        SickPartitions sickPartitions = new SickPartitions();
        deployable.addHealthCheck(new SickThreadsHealthCheck(deployable.config(AmzaSickThreadsHealthConfig.class), sickThreads));
        deployable.addHealthCheck(new SickPartitionsHealthCheck(sickPartitions));

        RingMember ringMember = new RingMember(Strings.padStart(String.valueOf(instanceId), 5, '0') + "_" + instanceKey);
        RingHost ringHost = new RingHost(datacenterName, rackName, hostName, port);

        TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceId),
            idPacker, timestampProvider);

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        PartitionPropertyMarshaller partitionPropertyMarshaller = new PartitionPropertyMarshaller() {

            @Override
            public PartitionProperties fromBytes(byte[] bytes) {
                try {
                    return mapper.readValue(bytes, PartitionProperties.class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public byte[] toBytes(PartitionProperties partitionProperties) {
                try {
                    return mapper.writeValueAsBytes(partitionProperties);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        BinaryPrimaryRowMarshaller primaryRowMarshaller = new BinaryPrimaryRowMarshaller(); // hehe you cant change this :)
        BinaryHighwaterRowMarshaller highwaterRowMarshaller = new BinaryHighwaterRowMarshaller(baInterner);

        TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
        TenantAwareHttpClient<String> ringClient = tenantRoutingHttpClientInitializer.builder(
            deployable.getTenantRoutingProvider().getConnections(serviceName, "main", 10_000), // TODO config
            clientHealthProvider)
            .deadAfterNErrors(10)
            .checkDeadEveryNMillis(10_000)
            .maxConnections(1_000)
            .socketTimeoutInMillis(60_000)
            .build(); // TODO expose to conf

        RowsTakerFactory rowsTakerFactory = () -> new HttpRowsTaker(amzaStats, ringClient, mapper, baInterner);
        AvailableRowsTaker availableRowsTaker = new HttpAvailableRowsTaker(ringClient, baInterner);
        AquariumStats aquariumStats = new AquariumStats();

        AmzaService amzaService = new AmzaServiceInitializer().initialize(amzaServiceConfig,
            baInterner,
            aquariumStats,
            amzaStats,
            sickThreads,
            sickPartitions,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            ringMember,
            ringHost,
            blacklistRingMembers,
            orderIdProvider,
            idPacker,
            partitionPropertyMarshaller,
            (workingIndexDirectories,
                indexProviderRegistry,
                ephemeralRowIOProvider,
                persistentRowIOProvider,
                partitionStripeFunction) -> {

                indexProviderRegistry.register(
                    new BerkeleyDBWALIndexProvider(BerkeleyDBWALIndexProvider.INDEX_CLASS_NAME,
                        partitionStripeFunction,
                        workingIndexDirectories),
                    persistentRowIOProvider);

                indexProviderRegistry.register(new LABPointerIndexWALIndexProvider(indexConfig,
                        LABPointerIndexWALIndexProvider.INDEX_CLASS_NAME,
                        partitionStripeFunction,
                        workingIndexDirectories),
                    persistentRowIOProvider);
            },
            availableRowsTaker,
            rowsTakerFactory,
            Optional.<TakeFailureListener>absent(),
            allRowChanges);

        RoutingBirdAmzaDiscovery routingBirdAmzaDiscovery = null;
        if (useAmzaDiscovery) {
            routingBirdAmzaDiscovery = new RoutingBirdAmzaDiscovery(deployable,
                serviceName,
                amzaService,
                amzaServiceConfig.discoveryIntervalMillis,
                blacklistRingMembers);
        }

        AmzaClientProvider<HttpClient, HttpClientException> clientProvider = new AmzaClientProvider<>(
            new HttpPartitionClientFactory(baInterner),
            new HttpPartitionHostsProvider(baInterner, ringClient, mapper),
            new RingHostHttpClientProvider(ringClient),
            Executors.newCachedThreadPool(),
            10_000, //TODO expose to conf
            -1,
            -1);

        new AmzaUIInitializer().initialize(clusterName, ringHost, amzaService, clientProvider, aquariumStats, amzaStats, timestampProvider, idPacker,
            new AmzaUIInitializer.InjectionCallback() {

                @Override
                public void addEndpoint(Class clazz) {
                    System.out.println("Adding endpoint=" + clazz);
                    deployable.addEndpoints(clazz);
                }

                @Override
                public void addInjectable(Class clazz, Object instance) {
                    System.out.println("Injecting " + clazz + " " + instance);
                    deployable.addInjectables(clazz, instance);
                }
            });

        deployable.addEndpoints(AmzaReplicationRestEndpoints.class);
        deployable.addInjectables(AmzaService.class, amzaService);
        deployable.addInjectables(AmzaRingWriter.class, amzaService.getRingWriter());
        deployable.addInjectables(AmzaRingReader.class, amzaService.getRingReader());
        deployable.addInjectables(AmzaInstance.class, amzaService);
        deployable.addInjectables(BAInterner.class, baInterner);

        if (bindClientEndpoints) {
            deployable.addEndpoints(AmzaClientRestEndpoints.class);
            deployable.addInjectables(AmzaRestClient.class, new AmzaRestClientHealthCheckDelegate(
                new AmzaClientService(amzaService.getRingReader(), amzaService.getRingWriter(), amzaService)));
        }

        Resource staticResource = new Resource(null)
            .addClasspathResource("resources/static/amza")
            .setDirectoryListingAllowed(false)
            .setContext("/static/amza");
        deployable.addResource(staticResource);

        return new Lifecycle(ringMember, ringHost, amzaService, routingBirdAmzaDiscovery);
    }

    public static class Lifecycle {

        public final RingMember ringMember;
        public final RingHost ringHost;
        public final AmzaService amzaService;
        public final RoutingBirdAmzaDiscovery routingBirdAmzaDiscovery;

        public Lifecycle(RingMember ringMember, RingHost ringHost, AmzaService amzaService, RoutingBirdAmzaDiscovery routingBirdAmzaDiscovery) {
            this.ringMember = ringMember;
            this.ringHost = ringHost;
            this.amzaService = amzaService;
            this.routingBirdAmzaDiscovery = routingBirdAmzaDiscovery;
        }

        public void startAmzaService() throws Exception {
            amzaService.start(ringMember, ringHost);

            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|      Amza Service Online");
            System.out.println("-----------------------------------------------------------------------");

        }

        public void startRoutingBirdAmzaDiscovery() {
            if (routingBirdAmzaDiscovery != null) {
                routingBirdAmzaDiscovery.start();

                System.out.println("-----------------------------------------------------------------------");
                System.out.println("|     Amza Service is in Routing Bird Discovery mode");
                System.out.println("-----------------------------------------------------------------------");
            }
        }
    }

}
