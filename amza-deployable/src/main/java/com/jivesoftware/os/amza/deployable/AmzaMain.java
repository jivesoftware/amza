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
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.berkeleydb.BerkeleyDBWALIndexProvider;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.amza.lsm.pointers.LSMPointerIndexWALIndexProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer;
import com.jivesoftware.os.amza.service.EmbeddedAmzaServiceInitializer;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.transport.http.replication.HttpAvailableRowsTaker;
import com.jivesoftware.os.amza.transport.http.replication.HttpRowsTaker;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaClientRestEndpoints;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaClientService;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaRestClient;
import com.jivesoftware.os.amza.ui.AmzaUIInitializer;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.api.ScheduledMinMaxHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.checkers.DiskFreeHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
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
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public class AmzaMain {

    static interface DiskFreeCheck extends ScheduledMinMaxHealthCheckConfig {

        @StringDefault("disk>free")
        @Override
        public String getName();

        @LongDefault(80)
        @Override
        public Long getMax();

    }

    public static void main(String[] args) throws Exception {
        new AmzaMain().run(args);
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
            amzaServiceConfig.compactTombstoneIfOlderThanNMillis = amzaConfig.getCompactTombstoneIfOlderThanNMillis();
            amzaServiceConfig.numberOfCompactorThreads = amzaConfig.getNumberOfCompactorThreads();
            amzaServiceConfig.numberOfTakerThreads = amzaConfig.getNumberOfTakerThreads();
            amzaServiceConfig.workingDirectories = workingDirs;

            final AmzaStats amzaStats = new AmzaStats();

            AvailableRowsTaker availableRowsTaker = new HttpAvailableRowsTaker(amzaStats);

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

            RingHost ringHost = new RingHost(instanceConfig.getHost(), instanceConfig.getMainPort());
            SnowflakeIdPacker idPacker = new SnowflakeIdPacker();
            TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName()),
                idPacker,
                new JiveEpochTimestampProvider());

            RingMember ringMember = new RingMember(
                Strings.padStart(String.valueOf(instanceConfig.getInstanceName()), 5, '0') + "_" + instanceConfig.getInstanceKey());

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
                () -> {
                    return new HttpRowsTaker(amzaStats);
                },
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
                new HttpPartitionClientFactory(),
                new HttpPartitionHostsProvider(httpClient, mapper),
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
            deployable.addInjectables(AmzaRestClient.class, new AmzaRestClientHealthCheckDelegate(
                new AmzaClientService(amzaService.getRingReader(), amzaService.getRingWriter(), amzaService)));

            new AmzaUIInitializer().initialize(instanceConfig.getClusterName(), ringHost, amzaService, clientProvider, amzaStats,
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

            amzaService.start();
            deployable.buildServer().start();
            serviceStartupHealthCheck.success();

            RoutingBirdAmzaDiscovery routingBirdAmzaDiscovery = new RoutingBirdAmzaDiscovery(deployable,
                instanceConfig.getServiceName(),
                amzaService,
                amzaConfig.getDiscoveryIntervalMillis());
            routingBirdAmzaDiscovery.start();

        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
