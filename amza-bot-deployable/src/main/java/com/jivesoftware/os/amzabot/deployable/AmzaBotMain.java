package com.jivesoftware.os.amzabot.deployable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.amzabot.deployable.AmzaBotHealthCheck.AmzaBotHealthCheckConfig;
import com.jivesoftware.os.amzabot.deployable.bot.AmzaBotCoalmineConfig;
import com.jivesoftware.os.amzabot.deployable.bot.AmzaBotCoalmineService;
import com.jivesoftware.os.amzabot.deployable.bot.AmzaBotRandomOpConfig;
import com.jivesoftware.os.amzabot.deployable.bot.AmzaBotRandomOpService;
import com.jivesoftware.os.amzabot.deployable.endpoint.AmzaBotCoalmineEndpoints;
import com.jivesoftware.os.amzabot.deployable.endpoint.AmzaBotEndpoints;
import com.jivesoftware.os.amzabot.deployable.endpoint.AmzaBotRandomOpEndpoints;
import com.jivesoftware.os.amzabot.deployable.ui.amzabot.AmzaBotUIEndpoints;
import com.jivesoftware.os.amzabot.deployable.ui.amzabot.AmzaBotUIInitializer;
import com.jivesoftware.os.amzabot.deployable.ui.amzabot.AmzaBotUIInitializer.AmzaBotUIServiceConfig;
import com.jivesoftware.os.amzabot.deployable.ui.amzabot.AmzaBotUIService;
import com.jivesoftware.os.amzabot.deployable.ui.health.UiEndpoints;
import com.jivesoftware.os.amzabot.deployable.ui.health.UiService;
import com.jivesoftware.os.amzabot.deployable.ui.health.UiServiceInitializer;
import com.jivesoftware.os.amzabot.deployable.ui.health.UiServiceInitializer.UiServiceConfig;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.DeployableHealthCheckRegistry;
import com.jivesoftware.os.routing.bird.deployable.ErrorHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.deployable.config.extractor.ConfigBinder;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.checkers.FileDescriptorCountHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCPauseHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.LoadAverageHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.routing.bird.health.checkers.SystemCpuHealthChecker;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import java.io.File;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Executors;

public class AmzaBotMain {

    public static void main(String[] args) throws Exception {
        new AmzaBotMain().run(args);
    }

    private void run(String[] args) throws Exception {

        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            ConfigBinder configBinder = new ConfigBinder(args);
            InstanceConfig instanceConfig = configBinder.bind(InstanceConfig.class);

            final Deployable deployable = new Deployable(args, configBinder, instanceConfig, null);
            HealthFactory.initialize(deployable::config, new DeployableHealthCheckRegistry(deployable));
            deployable.buildStatusReporter(null).start();
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new HasUI.UI("manage", "manage", "/manage/ui"),
                new HasUI.UI("Reset Errors", "manage", "/manage/resetErrors"),
                new HasUI.UI("Reset Health", "manage", "/manage/resetHealth"),
                new HasUI.UI("Tail", "manage", "/manage/tail"),
                new HasUI.UI("Thread Dump", "manage", "/manage/threadDump"),
                new HasUI.UI("Health", "manage", "/manage/ui"),
                new HasUI.UI("AmzaBot", "main", "/"))));

            AmzaKeyClearingHousePool amzaKeyClearingHousePool = new AmzaKeyClearingHousePool();
            AmzaBotHealthCheck amzaBotHealthCheck =
                new AmzaBotHealthCheck(instanceConfig,
                    deployable.config(AmzaBotHealthCheckConfig.class),
                    amzaKeyClearingHousePool);

            deployable.addHealthCheck(
                new GCPauseHealthChecker(deployable.config(GCPauseHealthChecker.GCPauseHealthCheckerConfig.class)),
                new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)),
                new SystemCpuHealthChecker(deployable.config(SystemCpuHealthChecker.SystemCpuHealthCheckerConfig.class)),
                new LoadAverageHealthChecker(deployable.config(LoadAverageHealthChecker.LoadAverageHealthCheckerConfig.class)),
                new FileDescriptorCountHealthChecker(deployable.config(FileDescriptorCountHealthChecker.FileDescriptorCountHealthCheckerConfig.class)),
                amzaBotHealthCheck,
                serviceStartupHealthCheck);
            deployable.addErrorHealthChecks(deployable.config(ErrorHealthCheckConfig.class));
            deployable.buildManageServer().start();

            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(instanceConfig.getInstanceKey(),
                HttpRequestHelperUtils.buildRequestHelper(false, false, null, instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(), 5_000, 100);

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> amzaClient = deployable.getTenantRoutingHttpClientInitializer().builder(
                deployable.getTenantRoutingProvider().getConnections("amza", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .socketTimeoutInMillis(60_000)
                .build(); // TODO expose to conf

            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            AmzaBotConfig amzaBotConfig = configBinder.bind(AmzaBotConfig.class);
            AmzaBotCoalmineConfig amzaBotCoalmineConfig = configBinder.bind(AmzaBotCoalmineConfig.class);
            AmzaBotRandomOpConfig amzaBotRandomOpConfig = configBinder.bind(AmzaBotRandomOpConfig.class);

            BAInterner interner = new BAInterner();
            AmzaClientProvider<HttpClient, HttpClientException> amzaClientProvider = new AmzaClientProvider<>(
                new HttpPartitionClientFactory(interner),
                new HttpPartitionHostsProvider(interner, amzaClient, objectMapper),
                new RingHostHttpClientProvider(amzaClient),
                Executors.newFixedThreadPool(amzaBotConfig.getAmzaCallerThreadPoolSize()),
                amzaBotConfig.getAmzaAwaitLeaderElectionForNMillis(),
                -1,
                -1);

            AmzaBotService amzaBotService = new AmzaBotService(
                amzaBotConfig,
                amzaClientProvider,
                () -> -1L,
                Durability.fsync_async,
                Consistency.leader_quorum,
                "amzabot-rest",
                amzaBotConfig.getRingSize());

            OrderIdProvider orderIdProviderRandomOp = () -> -1L;
            if (amzaBotRandomOpConfig.getClientOrdering()) {
                orderIdProviderRandomOp = new OrderIdProviderImpl(
                    new ConstantWriterIdProvider(instanceConfig.getInstanceName()));
            }

            AmzaBotRandomOpService amzaBotRandomOpService = new AmzaBotRandomOpService(
                amzaBotRandomOpConfig,
                new AmzaBotService(
                    amzaBotConfig,
                    amzaClientProvider,
                    orderIdProviderRandomOp,
                    Durability.valueOf(amzaBotRandomOpConfig.getDurability()),
                    Consistency.valueOf(amzaBotRandomOpConfig.getConsistency()),
                    "amzabot-randomops-" + UUID.randomUUID().toString(),
                    amzaBotRandomOpConfig.getRingSize()),
                amzaKeyClearingHousePool.genAmzaKeyClearingHouse());
            amzaBotRandomOpService.start();

            AmzaBotCoalmineService amzaBotCoalmineService = new AmzaBotCoalmineService(
                instanceConfig,
                amzaBotConfig,
                amzaBotCoalmineConfig,
                amzaClientProvider,
                amzaKeyClearingHousePool);
            amzaBotCoalmineService.start();

            String cacheToken = String.valueOf(System.currentTimeMillis());
            AmzaBotUIServiceConfig amzabotUIServiceConfig = deployable.config(AmzaBotUIServiceConfig.class);
            AmzaBotUIService amzabotUIService = new AmzaBotUIInitializer()
                .initialize(cacheToken, amzabotUIServiceConfig);

            UiServiceConfig uiServiceConfig = deployable.config(UiServiceConfig.class);
            UiService uiService = new UiServiceInitializer().initialize(cacheToken, uiServiceConfig);

            File staticResourceDir = new File(System.getProperty("user.dir"));
            Resource sourceTree = new Resource(staticResourceDir)
                .addResourcePath(amzabotUIServiceConfig.getPathToStaticResources())
                .setDirectoryListingAllowed(false)
                .setContext("/amzabot/static");

            deployable.addInjectables(AmzaBotService.class, amzaBotService);
            deployable.addInjectables(AmzaBotRandomOpService.class, amzaBotRandomOpService);
            deployable.addInjectables(AmzaBotCoalmineService.class, amzaBotCoalmineService);

            deployable.addEndpoints(AmzaBotEndpoints.class);
            deployable.addEndpoints(AmzaBotRandomOpEndpoints.class);
            deployable.addEndpoints(AmzaBotCoalmineEndpoints.class);

            deployable.addEndpoints(AmzaBotUIEndpoints.class);
            deployable.addInjectables(AmzaBotUIService.class, amzabotUIService);

            deployable.addEndpoints(UiEndpoints.class);
            deployable.addInjectables(UiService.class, uiService);

            deployable.addResource(sourceTree);
            deployable.enableSwagger("com.jivesoftware.os.amzabot.deployable.endpoints");

            deployable.buildServer().start();
            clientHealthProvider.start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Failure encountered during startup.", t);
        }
    }

}
