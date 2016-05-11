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
import com.google.common.base.Strings;
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
import com.jivesoftware.os.amza.service.EmbeddedAmzaServiceInitializer;
import com.jivesoftware.os.amza.service.SickPartitions;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.replication.http.AmzaClientService;
import com.jivesoftware.os.amza.service.replication.http.AmzaRestClient;
import com.jivesoftware.os.amza.service.replication.http.HttpAvailableRowsTaker;
import com.jivesoftware.os.amza.service.replication.http.HttpRowsTaker;
import com.jivesoftware.os.amza.service.replication.http.endpoints.AmzaClientRestEndpoints;
import com.jivesoftware.os.amza.service.replication.http.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.ui.AmzaUIInitializer;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.ErrorHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.api.ScheduledMinMaxHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.api.SickHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.checkers.DiskFreeHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreadsHealthCheck;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Executors;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public class AmzaMain {

    public static void main(String[] args) throws Exception {
        new AmzaMain().run(args);
    }

    interface DiskFreeCheck extends ScheduledMinMaxHealthCheckConfig {

        @StringDefault("disk>free")
        @Override
        public String getName();

        @LongDefault(80)
        @Override
        public Long getMax();

    }

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

    public void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);
            HealthFactory.initialize(deployable::config, new HealthCheckRegistry() {

                @Override
                public void register(HealthChecker healthChecker) {
                    deployable.addHealthCheck(healthChecker);
                }

                @Override
                public void unregister(HealthChecker healthChecker) {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            });

            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new HasUI.UI("manage", "manage", "/manage/ui"),
                new HasUI.UI("Amza", "main", "/amza"))));

            deployable.buildStatusReporter(null).start();
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.addErrorHealthChecks(deployable.config(ErrorHealthCheckConfig.class));
            deployable.buildManageServer().start();

            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

            AmzaConfig amzaConfig = deployable.config(AmzaConfig.class);

            String[] workingDirs = amzaConfig.getWorkingDirs().split(",");
            File[] paths = new File[workingDirs.length];
            for (int i = 0; i < workingDirs.length; i++) {
                paths[i] = new File(workingDirs[i].trim());
            }

            HealthFactory.scheduleHealthChecker(DiskFreeCheck.class,
                config1 -> (HealthChecker) new DiskFreeHealthChecker(config1, paths));

            final AmzaServiceInitializer.AmzaServiceConfig amzaServiceConfig = new AmzaServiceInitializer.AmzaServiceConfig();
            amzaServiceConfig.checkIfCompactionIsNeededIntervalInMillis = amzaConfig.getCheckIfCompactionIsNeededIntervalInMillis();
            amzaServiceConfig.numberOfTakerThreads = amzaConfig.getNumberOfTakerThreads();
            amzaServiceConfig.workingDirectories = workingDirs;
            amzaServiceConfig.asyncFsyncIntervalMillis = amzaConfig.getAsyncFsyncIntervalMillis();
            amzaServiceConfig.useMemMap = amzaConfig.getUseMemMap();

            amzaServiceConfig.tombstoneCompactionFactor = amzaConfig.getTombstoneCompactionFactor();
            amzaServiceConfig.rebalanceIfImbalanceGreaterThanNBytes = amzaConfig.getRebalanceIfImbalanceGreaterThanNBytes();
            amzaServiceConfig.rebalanceableEveryNMillis = amzaConfig.getRebalanceableEveryNMillis();
            amzaServiceConfig.interruptBlockingReadsIfLingersForNMillis = amzaConfig.getInterruptBlockingReadsIfLingersForNMillis();

            final AmzaStats amzaStats = new AmzaStats();
            final SickThreads sickThreads = new SickThreads();
            final SickPartitions sickPartitions = new SickPartitions();

            deployable.addHealthCheck(new SickThreadsHealthCheck(deployable.config(AmzaSickThreadsHealthConfig.class), sickThreads));
            deployable.addHealthCheck(new SickPartitionsHealthCheck(sickPartitions));

            BAInterner interner = new BAInterner();
            AvailableRowsTaker availableRowsTaker = new HttpAvailableRowsTaker(interner, "mainTaker",
                amzaServiceConfig.interruptBlockingReadsIfLingersForNMillis);

            final ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            mapper.configure(SerializationFeature.INDENT_OUTPUT, false);
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

            RingHost ringHost = new RingHost(instanceConfig.getDatacenter(), instanceConfig.getRack(), instanceConfig.getHost(), instanceConfig.getMainPort());
            SnowflakeIdPacker idPacker = new SnowflakeIdPacker();
            JiveEpochTimestampProvider timestampProvider = new JiveEpochTimestampProvider();
            TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName()),
                idPacker, timestampProvider);

            RingMember ringMember = new RingMember(
                Strings.padStart(String.valueOf(instanceConfig.getInstanceName()), 5, '0') + "_" + instanceConfig.getInstanceKey());

            LABPointerIndexConfig labConfig = deployable.config(LABPointerIndexConfig.class);

            AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(amzaServiceConfig,
                interner,
                amzaStats,
                sickThreads,
                sickPartitions,
                ringMember,
                ringHost,
                orderIdProvider,
                idPacker,
                partitionPropertyMarshaller,
                (workingIndexDirectories, indexProviderRegistry, ephemeralRowIOProvider, persistentRowIOProvider, numberOfStripes) -> {
                    indexProviderRegistry.register(
                        new BerkeleyDBWALIndexProvider(BerkeleyDBWALIndexProvider.INDEX_CLASS_NAME,
                            numberOfStripes,
                            workingIndexDirectories),
                        persistentRowIOProvider);

                    indexProviderRegistry.register(
                        new LABPointerIndexWALIndexProvider(labConfig,
                            LABPointerIndexWALIndexProvider.INDEX_CLASS_NAME,
                            numberOfStripes,
                            workingIndexDirectories),
                        persistentRowIOProvider);

                },
                availableRowsTaker,
                (name) -> new HttpRowsTaker(amzaStats, interner, name, amzaServiceConfig.interruptBlockingReadsIfLingersForNMillis),
                Optional.<TakeFailureListener>absent(),
                (RowsChanged changes) -> {
                });

            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(instanceConfig.getInstanceKey(),
                HttpRequestHelperUtils.buildRequestHelper(instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(), 5_000, 100);

            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
            TenantAwareHttpClient<String> httpClient = tenantRoutingHttpClientInitializer.initialize(
                deployable.getTenantRoutingProvider().getConnections(instanceConfig.getServiceName(), "main"),
                clientHealthProvider,
                10,
                10_000); // TODO expose to conf

            AmzaClientProvider<HttpClient, HttpClientException> clientProvider = new AmzaClientProvider<>(
                new HttpPartitionClientFactory(interner),
                new HttpPartitionHostsProvider(interner, httpClient, mapper),
                new RingHostHttpClientProvider(httpClient),
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("client-%d").build()),
                10_000); //TODO expose to conf

            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|      Tcp Replication Service Online");
            System.out.println("-----------------------------------------------------------------------");

            deployable.addEndpoints(com.jivesoftware.os.amza.deployable.AmzaEndpoints.class);
            deployable.addInjectables(AmzaService.class, amzaService);
            deployable.addEndpoints(AmzaReplicationRestEndpoints.class);
            deployable.addInjectables(AmzaInstance.class, amzaService);
            deployable.addEndpoints(AmzaClientRestEndpoints.class);
            deployable.addInjectables(BAInterner.class, interner);
            deployable.addInjectables(AmzaRestClient.class, new AmzaRestClientHealthCheckDelegate(
                new AmzaClientService(amzaService.getRingReader(), amzaService.getRingWriter(), amzaService)));

            new AmzaUIInitializer().initialize(instanceConfig.getClusterName(), ringHost, amzaService, clientProvider, amzaStats, timestampProvider, idPacker,
                new AmzaUIInitializer.InjectionCallback() {

                @Override
                public void addEndpoint(Class clazz) {
                    deployable.addEndpoints(clazz);
                }

                @Override
                public void addInjectable(Class clazz, Object instance) {
                    deployable.addInjectables(clazz, instance);
                }
            });

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                .addResourcePath("resources/static")
                .setContext("/static");
            deployable.addResource(sourceTree);

            Resource staticResource = new Resource(null)
                .addClasspathResource("resources/static/amza")
                .setContext("/static/amza");
            deployable.addResource(staticResource);

            RoutingBirdAmzaDiscovery routingBirdAmzaDiscovery = new RoutingBirdAmzaDiscovery(deployable,
                instanceConfig.getServiceName(),
                amzaService,
                amzaConfig.getDiscoveryIntervalMillis());

            amzaService.start(ringMember, ringHost);

            routingBirdAmzaDiscovery.start();

            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|     Amza Service is in Routing Bird Discovery mode");
            System.out.println("-----------------------------------------------------------------------");

            deployable.buildServer().start();
            serviceStartupHealthCheck.success();

        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
