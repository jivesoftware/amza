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
package com.jivesoftware.os.amza.ui.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.service.AmzaInstance;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.ui.soy.SoyService;
import com.jivesoftware.os.amza.ui.utils.MinMaxLong;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

/**
 * @author jonathan.colt
 */
@Singleton
@Path("/amza/")
public class AmzaUIEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaClusterName amzaClusterName;
    private final ObjectMapper mapper = new ObjectMapper();

    public static class AmzaClusterName {

        private final String name;

        public AmzaClusterName(String name) {
            this.name = name;
        }

    }

    private final AmzaInstance amzaInstance;
    private final AmzaStats amzaStats;
    private final RingHost host;
    private final AmzaRingReader ringReader;
    private final SoyService soyService;

    public AmzaUIEndpoints(@Context AmzaClusterName amzaClusterName,
        @Context AmzaStats amzaStats,
        @Context RingHost host,
        @Context AmzaRingReader ringReader,
        @Context AmzaInstance amzaInstance,
        @Context SoyService soyService) {
        this.amzaClusterName = amzaClusterName;
        this.amzaStats = amzaStats;
        this.host = host;
        this.amzaInstance = amzaInstance;
        this.ringReader = ringReader;
        this.soyService = soyService;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/chord")
    public Response chord() {
        try {
            RingTopology ring = ringReader.getRing(AmzaRingReader.SYSTEM_RING);
            List<List<Integer>> matrix = new ArrayList<>();
            for (RingMemberAndHost e1 : ring.entries) {
                List<Integer> weights = new ArrayList<>();
                for (RingMemberAndHost e2 : ring.entries) {
                    if (host.equals(e1.ringHost)) {
                        if (!host.equals(e2.ringHost)) {
                            weights.add((int) (amzaStats.getTotalTakes(e2.ringMember)));
                        } else {
                            weights.add(0);
                        }
                    } else if (host.equals(e2.ringHost)) {
                        weights.add((int) (amzaStats.getTotalTakes(e1.ringMember)));
                    } else {
                        weights.add(0);
                    }
                }
                for (RingMemberAndHost e2 : ring.entries) {
                    weights.add(0);
                }
                matrix.add(weights);

                weights = new ArrayList<>();
                for (RingMemberAndHost e2 : ring.entries) {
                    weights.add(0);
                }
                for (RingMemberAndHost e2 : ring.entries) {
                    if (host.equals(e1.ringHost)) {
                        if (!host.equals(e2.ringHost)) {
                            weights.add(0); //TODO FIX (int) (amzaStats.getTotalOffered(r2.getKey())));
                        } else {
                            weights.add(0);
                        }
                    } else if (host.equals(e2.ringHost)) {
                        weights.add(0); //TODO FIX (int) (amzaStats.getTotalOffered(r.getKey())));
                    } else {
                        weights.add(0);
                    }
                }
                matrix.add(weights);
            }
            return Response.ok(mapper.writeValueAsString(matrix), MediaType.APPLICATION_JSON).build();
        } catch (Exception x) {
            LOG.error("Failed genrating chords.", x);
            return Response.serverError().build();
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/arc")
    public Response arc() {
        try {
            RingTopology ring = ringReader.getRing(AmzaRingReader.SYSTEM_RING);

            Map<String, Integer> index = new HashMap<>();
            Map<String, Object> arcData = new HashMap<>();
            int i = 0;
            int g = 1;
            List<Map<String, Object>> nodes = new ArrayList<>();
            for (RingMemberAndHost entry : ring.entries) {
                String name = entry.ringHost.getHost() + ":" + entry.ringHost.getPort();
                addNode(nodes, index, name + "-received", i, g);
                i++;
                addNode(nodes, index, name + "-take", i, g);
                i++;
                if (entry.ringHost.equals(host)) {
                    addNode(nodes, index, name, i, g);
                    i++;
                }
                addNode(nodes, index, name + "-took", i, g);
                i++;
                addNode(nodes, index, name + "-replicate", i, g);
                i++;
                for (int b = 0; b < 10; b++) {
                    addNode(nodes, index, "", i, 0);
                    i++;
                }
                g++;

            }

            MinMaxLong range = new MinMaxLong();
            range.value(0);
            String sourceName = hostAsString(host);
            long[] offered = new long[ring.entries.size()];
            long[] takes = new long[ring.entries.size()];

            i = 0;
            for (RingMemberAndHost entry : ring.entries) {
                long totalOffered = 0; //TODO FIX amzaStats.getTotalOffered(r.getKey());
                offered[i] = totalOffered;
                if (totalOffered > 0) {
                    range.value(totalOffered);
                }

                long totalTakes = amzaStats.getTotalTakes(entry.ringMember);
                takes[i] = totalTakes;
                if (totalTakes > 0) {
                    range.value(totalOffered);
                }
                long total = totalOffered + totalTakes;
                if (total > 0) {
                    range.value(total);
                }
                i++;
            }

            List<Map<String, Integer>> links = new ArrayList<>();
            Integer root = index.get(sourceName);
            i = 0;
            for (RingMemberAndHost entry : ring.entries) {
                long totalOffered = offered[i];
                long totalTakes = takes[i];
                long total = totalOffered + totalTakes; // TODO use applied from replicated vs took
                if (totalOffered > 0) {
                    Integer source = index.get(sourceName + "-replicate");
                    Integer target = index.get(hostAsString(entry.ringHost) + "-received");
                    addLink(source, target, range, totalOffered, links);
                    addLink(root, target, range, total, links);

                }

                if (totalTakes > 0) {
                    Integer source = index.get(sourceName + "-take");
                    Integer target = index.get(hostAsString(entry.ringHost) + "-took");
                    addLink(source, target, range, totalTakes, links);
                    addLink(root, target, range, total, links);
                }
                i++;
            }
            arcData.put("nodes", nodes);
            arcData.put("links", links);

            return Response.ok(mapper.writeValueAsString(arcData), MediaType.APPLICATION_JSON).build();
        } catch (Exception x) {
            LOG.error("Failed genrating chords.", x);
            return Response.serverError().build();
        }
    }

    private void addLink(Integer source, Integer target, MinMaxLong range, long value, List<Map<String, Integer>> links) {
        Map<String, Integer> map = new HashMap<>();
        map.put("source", source);
        map.put("target", target);
        map.put("value", 1 + (int) (range.zeroToOne(value) * 10));
        links.add(map);
    }

    private String hostAsString(RingHost ringHost) {
        return ringHost.getHost() + ":" + ringHost.getPort();
    }

    private void addNode(List<Map<String, Object>> nodes, Map<String, Integer> index, String nodeName, int i, int g) {
        index.put(nodeName, i);
        Map<String, Object> node = new HashMap<>();
        node.put("nodeName", nodeName);
        node.put("group", g);
        nodes.add(node);
    }

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/download")
    public StreamingOutput pull() throws Exception {
        return (OutputStream os) -> {
            try {
                File f = new File(System.getProperty("user.dir"), "amza.jar");

                try {
                    byte[] buf = new byte[8192];
                    InputStream is = new FileInputStream(f);
                    int c = 0;
                    while ((c = is.read(buf, 0, buf.length)) > 0) {
                        os.write(buf, 0, c);
                        os.flush();
                    }
                    os.close();
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                throw new WebApplicationException(e);
            }
        };
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get(@Context UriInfo uriInfo) {
        String rendered = soyService.render(uriInfo.getAbsolutePath() + "propagator/download", amzaClusterName.name);
        return Response.ok(rendered).build();
    }

}
