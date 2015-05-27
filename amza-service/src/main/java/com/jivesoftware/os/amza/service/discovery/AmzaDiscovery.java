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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.AmzaHostRing;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RingMember;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * This is still a work in progress.
 */
public class AmzaDiscovery {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaHostRing amzaRing;
    private final String clusterName;
    private final InetAddress multicastGroup;
    private final int multicastPort;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder().setNameFormat("discovery-%d").build());

    public AmzaDiscovery(AmzaHostRing amzaRing,
        String clusterName,
        String multicastGroup,
        int multicastPort) throws UnknownHostException {
        this.amzaRing = amzaRing;
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
                try (MulticastSocket socket = new MulticastSocket(multicastPort)) {
                    socket.joinGroup(multicastGroup);
                    try {
                        byte[] buf = new byte[512];
                        while (true) {
                            DatagramPacket packet = new DatagramPacket(buf, buf.length);
                            socket.receive(packet);
                            String received = new String(packet.getData(), 0, packet.getLength());
                            List<String> clusterMemberHostPort = Lists.newArrayList(Splitter.on('|').split(received));
                            if (clusterMemberHostPort.size() == 4 && clusterMemberHostPort.get(0) != null && clusterMemberHostPort.get(0).equals(clusterName)) {
                                LOG.debug("received:" + clusterMemberHostPort);
                                String member = clusterMemberHostPort.get(1).trim();
                                String host = clusterMemberHostPort.get(2).trim();
                                int port = Integer.parseInt(clusterMemberHostPort.get(3).trim());
                                RingMember ringMember = new RingMember(member);
                                RingHost anotherRingHost = new RingHost(host, port);
                                NavigableMap<RingMember, RingHost> ring = amzaRing.getRing("system");
                                RingHost ringHost = ring.get(ringMember);
                                if (ringHost == null) {
                                    LOG.info("Adding ringMember:" + ringMember + " on host:" + anotherRingHost + " to cluster: " + clusterName);
                                    amzaRing.register(ringMember, anotherRingHost);
                                    amzaRing.addRingMember("system", ringMember);
                                } else if (!ringHost.equals(anotherRingHost)) {
                                    LOG.info("Updating ringMember:" + ringMember + " on host:" + anotherRingHost + " for cluster:" + clusterName);
                                    amzaRing.register(ringMember, anotherRingHost);
                                }
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
                RingMember ringMember = amzaRing.getRingMember();
                RingHost ringHost = amzaRing.getRingHost();
                if (ringHost != RingHost.UNKNOWN_RING_HOST) {
                    message = (clusterName + "|" + ringMember.getMember() + "|" + ringHost.getHost() + "|" + ringHost.getPort());
                    byte[] buf = new byte[512];
                    byte[] rawMessage = message.getBytes();
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
