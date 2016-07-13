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
package com.jivesoftware.os.amza.service.discovery;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.api.ring.TimestampedRingHost;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.AmzaRingStoreWriter;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This is still a work in progress.
 */
public class AmzaDiscovery {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaRingStoreReader ringStoreReader;
    private final AmzaRingStoreWriter ringStoreWriter;
    private final String clusterName;
    private final InetAddress multicastGroup;
    private final int multicastPort;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder().setNameFormat("discovery-%d").build());

    public AmzaDiscovery(AmzaRingStoreReader ringStoreReader,
        AmzaRingStoreWriter ringStoreWriter,
        String clusterName,
        String multicastGroup,
        int multicastPort) throws UnknownHostException {

        this.ringStoreReader = ringStoreReader;
        this.ringStoreWriter = ringStoreWriter;
        this.clusterName = clusterName;
        this.multicastGroup = InetAddress.getByName(multicastGroup);
        this.multicastPort = multicastPort;
    }

    public void start() throws IOException {
        executor.scheduleWithFixedDelay(new MulticastReceiver(), 0, 10, TimeUnit.SECONDS);
        executor.scheduleWithFixedDelay(new MulticastBroadcaster(), 0, 10, TimeUnit.SECONDS);
    }

    class MulticastReceiver implements Runnable {

        @Override
        public void run() {
            try {
                Set<RingMember> allMemberSeen = new HashSet<>();
                long timeout = TimeUnit.SECONDS.toMillis(30); // TODO expose to config
                try (MulticastSocket socket = new MulticastSocket(multicastPort)) {
                    socket.setSoTimeout((int) timeout);
                    socket.joinGroup(multicastGroup);
                    try {
                        byte[] buf = new byte[512];
                        long startTime = System.currentTimeMillis();
                        while (true) {
                            DatagramPacket packet = new DatagramPacket(buf, buf.length);
                            try {
                                socket.receive(packet);
                            } catch (Exception x) {
                                LOG.warn("No data.");
                                continue;
                            }
                            String received = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                            List<String> clusterMemberHostPort = Lists.newArrayList(Splitter.on('|').split(received));
                            if (clusterMemberHostPort.size() == 4 && clusterMemberHostPort.get(0) != null && clusterMemberHostPort.get(0).equals(clusterName)) {
                                LOG.debug("received:" + clusterMemberHostPort);
                                String member = clusterMemberHostPort.get(1).trim();
                                String datacenter = clusterMemberHostPort.get(2).trim();
                                String rack = clusterMemberHostPort.get(3).trim();
                                String host = clusterMemberHostPort.get(4).trim();
                                int port = Integer.parseInt(clusterMemberHostPort.get(5).trim());
                                long timestampId = Long.parseLong(clusterMemberHostPort.get(6).trim());
                                RingMember ringMember = new RingMember(member);
                                RingHost anotherRingHost = new RingHost(datacenter, rack, host, port);
                                RingTopology ring = ringStoreReader.getRing(AmzaRingReader.SYSTEM_RING);
                                //TODO this is a little heavy handed
                                RingMemberAndHost entry = Iterables.find(ring.entries, input -> input.ringMember.equals(ringMember));
                                if (entry == null) {
                                    LOG.info("Adding ringMember:" + ringMember + " on host:" + anotherRingHost + " to cluster: " + clusterName);
                                    ringStoreWriter.register(ringMember, anotherRingHost, timestampId, false);
                                    allMemberSeen.add(ringMember);
                                } else if (!entry.ringHost.equals(anotherRingHost)) {
                                    LOG.info("Updating ringMember:" + ringMember + " on host:" + anotherRingHost + " for cluster:" + clusterName);
                                    ringStoreWriter.register(ringMember, anotherRingHost, timestampId, false);
                                    allMemberSeen.add(ringMember);
                                }
                            }
                            long elapse = System.currentTimeMillis() - startTime;
                            if (elapse > timeout && allMemberSeen.size() <= 1) {
                                if (!allMemberSeen.contains(ringStoreReader.getRingMember())) {
                                    LOG.error("We have not seen our own multicast.");
                                }
                                LOG.error("We have not discovered any other members, elapsed:{} multicastGroup:{} multicastPort:{}",
                                    elapse, multicastGroup, multicastPort);
                            }
                        }
                    } catch (Exception x) {
                        LOG.error("Failed to receive broadcast from  group:" + multicastGroup + " port:" + multicastPort, x);
                    } finally {
                        socket.leaveGroup(multicastGroup);
                    }
                }
            } catch (IOException x) {
                LOG.error("Issue with MulticastReceiver", x);
            }
        }
    }

    class MulticastBroadcaster implements Runnable {

        @Override
        public void run() {
            String message = "";
            try (MulticastSocket socket = new MulticastSocket()) {
                RingMember ringMember = ringStoreReader.getRingMember();
                TimestampedRingHost timestampedRingHost = ringStoreReader.getRingHost();
                if (timestampedRingHost.ringHost != RingHost.UNKNOWN_RING_HOST) {
                    message = (clusterName
                        + "|" + ringMember.getMember()
                        + "|" + timestampedRingHost.ringHost.getDatacenter()
                        + "|" + timestampedRingHost.ringHost.getRack()
                        + "|" + timestampedRingHost.ringHost.getHost()
                        + "|" + timestampedRingHost.ringHost.getPort()
                        + "|" + timestampedRingHost.timestampId);
                    byte[] buf = new byte[512];
                    byte[] rawMessage = message.getBytes(StandardCharsets.UTF_8);
                    System.arraycopy(rawMessage, 0, buf, 0, rawMessage.length);
                    DatagramPacket packet = new DatagramPacket(buf, buf.length, multicastGroup, multicastPort);
                    socket.send(packet);
                    LOG.debug("Sent:" + message);
                }
            } catch (Exception e) {
                LOG.error("Failed to receive broadcast. message:" + message + " to  group:" + multicastGroup + " port:" + multicastPort, e);
            }
        }
    }
}
