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
import com.google.common.base.Strings;
import com.jivesoftware.os.amza.berkeleydb.BerkeleyDBWALIndexProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer;
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
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.transport.http.replication.HttpUpdatesTaker;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.AmzaReplicationRestEndpoints;
import com.jivesoftware.os.amza.ui.AmzaUIInitializer;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import java.util.Arrays;

public class AmzaMain {

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
            deployable.buildServer().start();

            AmzaConfig amzaConfig = deployable.config(AmzaConfig.class);

            final String[] workingDirs = amzaConfig.getWorkingDirs().split(",");

            final AmzaServiceInitializer.AmzaServiceConfig amzaServiceConfig = new AmzaServiceInitializer.AmzaServiceConfig();
            amzaServiceConfig.checkIfCompactionIsNeededIntervalInMillis = amzaConfig.getCheckIfCompactionIsNeededIntervalInMillis();
            amzaServiceConfig.compactTombstoneIfOlderThanNMillis = amzaConfig.getCompactTombstoneIfOlderThanNMillis();
            amzaServiceConfig.numberOfCompactorThreads = amzaConfig.getNumberOfCompactorThreads();
            amzaServiceConfig.numberOfTakerThreads = amzaConfig.getNumberOfTakerThreads();
            amzaServiceConfig.takeFromNeighborsIntervalInMillis = amzaConfig.getTakeFromNeighborsIntervalInMillis();
            amzaServiceConfig.workingDirectories = workingDirs;

            final AmzaStats amzaStats = new AmzaStats();

            WALIndexProviderRegistry indexProviderRegistry = new WALIndexProviderRegistry();
            indexProviderRegistry.register("berkeleydb", new BerkeleyDBWALIndexProvider(workingDirs, workingDirs.length));

            HttpUpdatesTaker taker = new HttpUpdatesTaker(amzaStats);

            final ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            mapper.configure(SerializationFeature.INDENT_OUTPUT, false);
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

            RingHost ringHost = new RingHost(instanceConfig.getHost(), instanceConfig.getMainPort());
            final TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName()));

            RingMember ringMember = new RingMember(
                Strings.padStart(String.valueOf(instanceConfig.getInstanceName()), 5, '0') + "_" + instanceConfig.getInstanceKey());

            AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(amzaServiceConfig,
                amzaStats,
                ringMember,
                ringHost,
                orderIdProvider,
                partitionPropertyMarshaller,
                indexProviderRegistry,
                taker,
                Optional.<SendFailureListener>absent(),
                Optional.<TakeFailureListener>absent(), (RowsChanged changes) -> {
                });

            System.out.println("-----------------------------------------------------------------------");
            System.out.println("|      Tcp Replication Service Online");
            System.out.println("-----------------------------------------------------------------------");

            deployable.addEndpoints(com.jivesoftware.os.amza.deployable.AmzaEndpoints.class);
            deployable.addInjectables(AmzaService.class, amzaService);
            deployable.addEndpoints(AmzaReplicationRestEndpoints.class);
            deployable.addInjectables(AmzaInstance.class, amzaService);
            deployable.addInjectables(HighwaterStorage.class, amzaService.getHighwaterMarks());

            new AmzaUIInitializer().initialize(instanceConfig.getClusterName(), ringHost, amzaService, amzaStats, new AmzaUIInitializer.InjectionCallback() {

                @Override
                public void addEndpoint(Class clazz) {
                    deployable.addEndpoints(clazz);
                }

                @Override
                public void addInjectable(Class clazz, Object instance) {
                    deployable.addInjectables(clazz, instance);
                }
            });

            amzaService.start();
            deployable.buildServer().start();
            serviceStartupHealthCheck.success();

            if (amzaConfig.getAutoDiscoveryEnabled()) {
                AmzaDiscovery amzaDiscovery = new AmzaDiscovery(amzaService.getAmzaHostRing(),
                    instanceConfig.getClusterName(),
                    amzaConfig.getDiscoveryMulticastGroup(),
                    amzaConfig.getDiscoveryMulticastPort());

                amzaDiscovery.start();
                System.out.println("-----------------------------------------------------------------------");
                System.out.println("|      Amza Service Discovery Online");
                System.out.println("-----------------------------------------------------------------------");
            } else {
                System.out.println("-----------------------------------------------------------------------");
                System.out.println("|     Amza Service is in manual Discovery mode.  No cluster name was specified");
                System.out.println("-----------------------------------------------------------------------");
            }

        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
