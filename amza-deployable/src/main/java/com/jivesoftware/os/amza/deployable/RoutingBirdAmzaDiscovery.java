package com.jivesoftware.os.amza.deployable;

import com.google.common.base.Strings;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.AmzaRingStoreWriter;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptors;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.InstanceDescriptor;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author jonathan.colt
 */
public class RoutingBirdAmzaDiscovery implements Runnable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private final Deployable deployable;
    private final String serviceName;
    private final AmzaService amzaService;
    private final long discoveryIntervalMillis;

    public RoutingBirdAmzaDiscovery(Deployable deployable, String serviceName, AmzaService amzaService, long discoveryIntervalMillis) {
        this.deployable = deployable;
        this.serviceName = serviceName;
        this.amzaService = amzaService;
        this.discoveryIntervalMillis = discoveryIntervalMillis;
    }

    public void start() {
        scheduledExecutorService.scheduleWithFixedDelay(this, 0, discoveryIntervalMillis, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduledExecutorService.shutdownNow();
    }

    @Override
    public void run() {
        try {
            TenantRoutingProvider tenantRoutingProvider = deployable.getTenantRoutingProvider();
            TenantsServiceConnectionDescriptorProvider connections = tenantRoutingProvider.getConnections(serviceName, "main");
            ConnectionDescriptors selfConnections = connections.getConnections("");
            for (ConnectionDescriptor connectionDescriptor : selfConnections.getConnectionDescriptors()) {

                InstanceDescriptor routingInstanceDescriptor = connectionDescriptor.getInstanceDescriptor();
                RingMember routingRingMember = new RingMember(
                    Strings.padStart(String.valueOf(routingInstanceDescriptor.instanceName), 5, '0') + "_" + routingInstanceDescriptor.instanceKey);

                HostPort hostPort = connectionDescriptor.getHostPort();
                AmzaRingStoreWriter ringWriter = amzaService.getRingWriter();
                ringWriter.register(routingRingMember, new RingHost(hostPort.getHost(), hostPort.getPort()));
                ringWriter.addRingMember(AmzaRingReader.SYSTEM_RING, routingRingMember);

            }
        } catch (Exception x) {
            LOG.warn("Failed while calling routing bird discovery.", x);
        }
    }
}
