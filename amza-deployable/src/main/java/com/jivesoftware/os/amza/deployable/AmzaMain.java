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

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.embed.AmzaConfig;
import com.jivesoftware.os.amza.embed.EmbedAmzaServiceInitializer;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexConfig;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.ErrorHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.api.ScheduledMinMaxHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.checkers.DiskFreeHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.FileDescriptorCountHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCPauseHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.LoadAverageHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.routing.bird.health.checkers.SystemCpuHealthChecker;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import java.io.File;
import java.util.Arrays;
import java.util.Set;
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
            deployable.addHealthCheck(new GCPauseHealthChecker(deployable.config(GCPauseHealthChecker.GCPauseHealthCheckerConfig.class)));
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(new SystemCpuHealthChecker(deployable.config(SystemCpuHealthChecker.SystemCpuHealthCheckerConfig.class)));
            deployable.addHealthCheck(new LoadAverageHealthChecker(deployable.config(LoadAverageHealthChecker.LoadAverageHealthCheckerConfig.class)));
            deployable.addHealthCheck(
                new FileDescriptorCountHealthChecker(deployable.config(FileDescriptorCountHealthChecker.FileDescriptorCountHealthCheckerConfig.class)));
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

            amzaServiceConfig.takeSlowThresholdInMillis = amzaConfig.getTakeSlowThresholdInMillis();
            amzaServiceConfig.maxUpdatesBeforeDeltaStripeCompaction = amzaConfig.getMaxUpdatesBeforeDeltaStripeCompaction();
            amzaServiceConfig.tombstoneCompactionFactor = amzaConfig.getTombstoneCompactionFactor();
            amzaServiceConfig.rebalanceIfImbalanceGreaterThanNBytes = amzaConfig.getRebalanceIfImbalanceGreaterThanNBytes();
            amzaServiceConfig.rebalanceableEveryNMillis = amzaConfig.getRebalanceableEveryNMillis();
            amzaServiceConfig.interruptBlockingReadsIfLingersForNMillis = amzaConfig.getInterruptBlockingReadsIfLingersForNMillis();
            amzaServiceConfig.rackDistributionEnabled = amzaConfig.getRackDistributionEnabled();

            AmzaStats amzaStats = new AmzaStats();
            BAInterner interner = new BAInterner();
            RingHost ringHost = new RingHost(instanceConfig.getDatacenter(), instanceConfig.getRack(), instanceConfig.getHost(), instanceConfig.getMainPort());
            SnowflakeIdPacker idPacker = new SnowflakeIdPacker();
            JiveEpochTimestampProvider timestampProvider = new JiveEpochTimestampProvider();

            RingMember ringMember = new RingMember(
                Strings.padStart(String.valueOf(instanceConfig.getInstanceName()), 5, '0') + "_" + instanceConfig.getInstanceKey());

            LABPointerIndexConfig labConfig = deployable.config(LABPointerIndexConfig.class);

            String blacklist = amzaConfig.getBlacklistRingMembers();
            Set<RingMember> blacklistRingMembers = Sets.newHashSet();
            for (String b : blacklist != null ? blacklist.split("\\s*,\\s*") : new String[0]) {
                if (b != null) {
                    b = b.trim();
                    if (!b.isEmpty()) {
                        blacklistRingMembers.add(new RingMember(b));
                    }
                }
            }

            EmbedAmzaServiceInitializer.Lifecycle lifecycle = new EmbedAmzaServiceInitializer().initialize(deployable,
                instanceConfig.getRoutesHost(),
                instanceConfig.getRoutesPort(),
                instanceConfig.getConnectionsHealth(),
                instanceConfig.getInstanceName(),
                instanceConfig.getInstanceKey(),
                instanceConfig.getServiceName(),
                instanceConfig.getDatacenter(),
                instanceConfig.getRack(),
                instanceConfig.getHost(),
                instanceConfig.getMainPort(),
                instanceConfig.getClusterName(),
                amzaServiceConfig,
                labConfig,
                amzaStats,
                interner,
                idPacker,
                timestampProvider,
                blacklistRingMembers,
                true,
                true,
                (RowsChanged changes) -> {
                });


            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                .addResourcePath("resources/static")
                .setContext("/static");
            deployable.addResource(sourceTree);

            lifecycle.startAmzaService();
            lifecycle.startRoutingBirdAmzaDiscovery();
            deployable.buildServer().start();
            serviceStartupHealthCheck.success();

        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
