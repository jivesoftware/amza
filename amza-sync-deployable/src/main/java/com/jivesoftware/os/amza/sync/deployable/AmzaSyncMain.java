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
package com.jivesoftware.os.amza.sync.deployable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.amza.client.collection.AmzaMarshaller;
import com.jivesoftware.os.amza.sync.api.AmzaSyncPartitionConfig;
import com.jivesoftware.os.amza.sync.api.AmzaSyncPartitionTuple;
import com.jivesoftware.os.amza.sync.api.AmzaSyncSenderConfig;
import com.jivesoftware.os.amza.sync.deployable.endpoints.AmzaSyncApiEndpoints;
import com.jivesoftware.os.amza.sync.deployable.endpoints.AmzaSyncEndpoints;
import com.jivesoftware.os.amza.sync.deployable.oauth.AmzaSyncOAuthValidatorInitializer;
import com.jivesoftware.os.amza.sync.deployable.oauth.AmzaSyncOAuthValidatorInitializer.AmzaSyncOAuthValidatorConfig;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.amza.ui.soy.SoyRendererInitializer;
import com.jivesoftware.os.amza.ui.soy.SoyRendererInitializer.SoyRendererConfig;
import com.jivesoftware.os.aquarium.AquariumStats;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.DeployableHealthCheckRegistry;
import com.jivesoftware.os.routing.bird.deployable.ErrorHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.endpoints.base.FullyOnlineVersion;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI.UI;
import com.jivesoftware.os.routing.bird.endpoints.base.LoadBalancerHealthCheckEndpoints;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.checkers.FileDescriptorCountHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCPauseHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.LoadAverageHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.routing.bird.health.checkers.SystemCpuHealthChecker;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.oauth.validator.AuthValidator;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptors;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.glassfish.jersey.oauth1.signature.OAuth1Request;
import org.glassfish.jersey.oauth1.signature.OAuth1Signature;

public class AmzaSyncMain {

    public static void main(String[] args) throws Exception {
        new AmzaSyncMain().run(args);
    }

    public void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);
            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);
            HealthFactory.initialize(deployable::config, new DeployableHealthCheckRegistry(deployable));
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new UI("Sync", "main", "/ui"))));
            deployable.addHealthCheck(new GCPauseHealthChecker(deployable.config(GCPauseHealthChecker.GCPauseHealthCheckerConfig.class)));
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(new SystemCpuHealthChecker(deployable.config(SystemCpuHealthChecker.SystemCpuHealthCheckerConfig.class)));
            deployable.addHealthCheck(new LoadAverageHealthChecker(deployable.config(LoadAverageHealthChecker.LoadAverageHealthCheckerConfig.class)));
            deployable.addHealthCheck(
                new FileDescriptorCountHealthChecker(deployable.config(FileDescriptorCountHealthChecker.FileDescriptorCountHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.addErrorHealthChecks(deployable.config(ErrorHealthCheckConfig.class));
            deployable.addManageInjectables(FullyOnlineVersion.class, (FullyOnlineVersion) () -> {
                if (serviceStartupHealthCheck.startupHasSucceeded()) {
                    return instanceConfig.getVersion();
                } else {
                    return null;
                }
            });
            deployable.buildManageServer().start();


            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(instanceConfig.getInstanceKey(),
                HttpRequestHelperUtils.buildRequestHelper(false, false, null, instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(), 5_000, 100);

            TenantRoutingProvider tenantRoutingProvider = deployable.getTenantRoutingProvider();

            TenantsServiceConnectionDescriptorProvider syncDescriptorProvider = tenantRoutingProvider
                .getConnections(instanceConfig.getServiceName(), "main", 10_000); // TODO config

            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = deployable.getTenantRoutingHttpClientInitializer();

            TenantAwareHttpClient<String> amzaClient = tenantRoutingHttpClientInitializer.builder(
                deployable.getTenantRoutingProvider().getConnections("amza", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .socketTimeoutInMillis(60_000)
                .build(); // TODO expose to conf

            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

            AmzaSyncConfig syncConfig = deployable.config(AmzaSyncConfig.class);

            BAInterner interner = new BAInterner();
            AmzaClientProvider<HttpClient, HttpClientException> amzaClientProvider = new AmzaClientProvider<>(
                new HttpPartitionClientFactory(interner),
                new HttpPartitionHostsProvider(interner, amzaClient, mapper),
                new RingHostHttpClientProvider(amzaClient),
                Executors.newFixedThreadPool(syncConfig.getAmzaCallerThreadPoolSize()),
                syncConfig.getAmzaAwaitLeaderElectionForNMillis(),
                -1,
                -1);

            TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(
                new ConstantWriterIdProvider(instanceConfig.getInstanceName()),
                new SnowflakeIdPacker(),
                new JiveEpochTimestampProvider());
            AmzaClientAquariumProvider amzaClientAquariumProvider = new AmzaClientAquariumProvider(new AquariumStats(),
                instanceConfig.getServiceName(),
                amzaClientProvider,
                orderIdProvider,
                new Member(instanceConfig.getInstanceKey().getBytes(StandardCharsets.UTF_8)),
                count -> {
                    ConnectionDescriptors descriptors = syncDescriptorProvider.getConnections("");
                    int ringSize = descriptors.getConnectionDescriptors().size();
                    return count > ringSize / 2;
                },
                () -> {
                    Set<Member> members = Sets.newHashSet();
                    ConnectionDescriptors descriptors = syncDescriptorProvider.getConnections("");
                    for (ConnectionDescriptor connectionDescriptor : descriptors.getConnectionDescriptors()) {
                        members.add(new Member(connectionDescriptor.getInstanceDescriptor().instanceKey.getBytes(StandardCharsets.UTF_8)));
                    }
                    return members;
                },
                128, //TODO config
                128, //TODO config
                5_000L, //TODO config
                100L, //TODO config
                60_000L, //TODO config
                10_000L, //TODO config
                Executors.newSingleThreadExecutor(),
                100L, //TODO config
                1_000L, //TODO config
                10_000L,//TODO config
                syncConfig.getAquariumUseSolutionLog());

            ObjectMapper miruSyncMapper = new ObjectMapper();
            miruSyncMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            miruSyncMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            AmzaMarshaller<String> stringMarshaller = new AmzaMarshaller<String>() {
                @Override
                public String fromBytes(byte[] bytes) throws Exception {
                    return new String(bytes, StandardCharsets.UTF_8);
                }

                @Override
                public byte[] toBytes(String value) throws Exception {
                    return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
                }
            };

            AmzaMarshaller<AmzaSyncSenderConfig> amzaSyncSenderConfigMarshaller = new AmzaMarshaller<AmzaSyncSenderConfig>() {
                @Override
                public AmzaSyncSenderConfig fromBytes(byte[] bytes) throws Exception {
                    return mapper.readValue(bytes,AmzaSyncSenderConfig.class);
                }

                @Override
                public byte[] toBytes(AmzaSyncSenderConfig value) throws Exception {
                    return mapper.writeValueAsBytes(value);
                }
            };

            AmzaClientProvider clientProvider = new AmzaClientProvider<>(
                new HttpPartitionClientFactory(interner),
                new HttpPartitionHostsProvider(interner, amzaClient, mapper),
                new RingHostHttpClientProvider(amzaClient),
                Executors.newCachedThreadPool(), //TODO expose to conf?
                30_000L, // TODO config
                -1,
                -1);


            AmzaSyncSenderMap senderConfigStorage = new AmzaSyncSenderMap(
                clientProvider,
                "amza-sync-sender-config",
                new PartitionProperties(Durability.fsync_async,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    false,
                    Consistency.leader_quorum,
                    true,
                    true,
                    false,
                    RowType.snappy_primary,
                    "lab",
                    -1,
                    null,
                    -1,
                    -1),
                stringMarshaller,
                amzaSyncSenderConfigMarshaller
            );

            AmzaMarshaller<AmzaSyncPartitionTuple> tupleMarshaller = new AmzaMarshaller<AmzaSyncPartitionTuple>() {
                @Override
                public AmzaSyncPartitionTuple fromBytes(byte[] bytes) throws Exception {
                    return AmzaSyncPartitionTuple.fromBytes(bytes, 0, interner);
                }

                @Override
                public byte[] toBytes(AmzaSyncPartitionTuple value) throws Exception {
                    return AmzaSyncPartitionTuple.toBytes(value);
                }
            };

            AmzaMarshaller<AmzaSyncPartitionConfig> partitionConfigMarshaller = new AmzaMarshaller<AmzaSyncPartitionConfig>() {
                @Override
                public AmzaSyncPartitionConfig fromBytes(byte[] bytes) throws Exception {
                    return mapper.readValue(bytes,AmzaSyncPartitionConfig.class);
                }

                @Override
                public byte[] toBytes(AmzaSyncPartitionConfig value) throws Exception {
                    return mapper.writeValueAsBytes(value);
                }
            };


            AmzaSyncPartitionConfigStorage syncPartitionConfigStorage = new AmzaSyncPartitionConfigStorage(
                clientProvider,
                "amza-sync-partitions-config-",
                new PartitionProperties(Durability.fsync_async,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    false,
                    Consistency.leader_quorum,
                    true,
                    true,
                    false,
                    RowType.snappy_primary,
                    "lab",
                    -1,
                    null,
                    -1,
                    -1),
                tupleMarshaller,
                partitionConfigMarshaller
            );

            AmzaSyncStats stats = new AmzaSyncStats();

            AmzaSyncReceiver syncReceiver = new AmzaSyncReceiver(amzaClientProvider, syncConfig.getSyncReceiverUseSolutionLog());

            AmzaSyncSenders syncSenders = null;
            if (syncConfig.getSyncSenderEnabled()) {
                ScheduledExecutorService executorService = Executors.newScheduledThreadPool(syncConfig.getSyncSendersThreadCount());
                syncSenders = new AmzaSyncSenders(stats,
                    syncConfig,
                    syncReceiver,
                    executorService,
                    amzaClientProvider,
                    amzaClientAquariumProvider,
                    interner,
                    mapper,
                    orderIdProvider,
                    senderConfigStorage,
                    syncPartitionConfigStorage,
                    30_000); // TODO config
            }

            amzaClientAquariumProvider.start();
            if (syncSenders != null) {
                syncSenders.start();
            }

            SoyRendererConfig rendererConfig = deployable.config(SoyRendererConfig.class);

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setDirectoryListingAllowed(false)
                .setContext("/ui/static");
            deployable.addResource(sourceTree);

            SoyRenderer renderer = new SoyRendererInitializer().initialize(rendererConfig);

            AmzaSyncUIService amzaSyncUIService = new AmzaSyncUIServiceInitializer().initialize(renderer,
                syncSenders,
                stats,
                syncConfig.getSyncSenderEnabled(),
                syncConfig.getSyncReceiverEnabled(),
                mapper);

            deployable.addEndpoints(LoadBalancerHealthCheckEndpoints.class);
            deployable.addNoAuth("/health/check");
            if (instanceConfig.getMainServiceAuthEnabled()) {
                if (syncConfig.getSyncReceiverEnabled()) {
                    AmzaSyncOAuthValidatorConfig oAuthValidatorConfig = deployable.config(AmzaSyncOAuthValidatorConfig.class);
                    AuthValidator<OAuth1Signature, OAuth1Request> syncOAuthValidator = new AmzaSyncOAuthValidatorInitializer()
                        .initialize(oAuthValidatorConfig);
                    deployable.addCustomOAuth(syncOAuthValidator, "/api/*");
                }
                deployable.addRouteOAuth("/amza/*", "/api/*");
                deployable.addSessionAuth("/ui/*", "/amza/*", "/api/*");
            } else {
                deployable.addNoAuth("/amza/*", "/api/*");
                deployable.addSessionAuth("/ui/*");
            }

            deployable.addEndpoints(AmzaSyncEndpoints.class);
            deployable.addInjectables(BAInterner.class, interner);
            if (syncSenders != null) {
                deployable.addInjectables(AmzaSyncSenders.class, syncSenders);
            }

            deployable.addEndpoints(AmzaSyncUIEndpoints.class);
            deployable.addInjectables(AmzaSyncUIService.class, amzaSyncUIService);

            if (syncConfig.getSyncReceiverEnabled()) {
                deployable.addEndpoints(AmzaSyncApiEndpoints.class);
                deployable.addInjectables(AmzaSyncReceiver.class, syncReceiver);
            }
            deployable.addInjectables(ObjectMapper.class, mapper);
            deployable.addInjectables(AmzaSyncSenderMap.class, senderConfigStorage);
            deployable.addInjectables(AmzaSyncPartitionConfigStorage.class, syncPartitionConfigStorage);

            deployable.buildServer().start();
            clientHealthProvider.start();
            serviceStartupHealthCheck.success();

        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }

    private PartitionName extractPartition(String s) {
        if (s.contains("/")) {
            String[] parts = s.split("/");
            return new PartitionName(false,
                parts[0].trim().getBytes(StandardCharsets.UTF_8),
                parts[1].trim().getBytes(StandardCharsets.UTF_8));
        } else {
            throw new IllegalArgumentException("Provide partition names in the format ringName/name");
        }
    }
}
