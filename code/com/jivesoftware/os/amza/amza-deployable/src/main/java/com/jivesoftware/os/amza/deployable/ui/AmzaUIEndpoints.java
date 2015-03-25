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
package com.jivesoftware.os.amza.deployable.ui;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.deployable.ui.soy.SoyService;
import com.jivesoftware.os.amza.deployable.ui.utils.MinMaxLong;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.AmzaRing;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
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
 *
 * @author jonathan.colt
 */
@Singleton
@Path("/")
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
    private final AmzaRing amzaRing;
    private final SoyService soyService;

    public AmzaUIEndpoints(@Context AmzaClusterName amzaClusterName,
        @Context AmzaStats amzaStats,
        @Context RingHost host,
        @Context AmzaRing amzaRing,
        @Context AmzaInstance amzaInstance,
        @Context SoyService soyService) {
        this.amzaClusterName = amzaClusterName;
        this.amzaStats = amzaStats;
        this.host = host;
        this.amzaInstance = amzaInstance;
        this.amzaRing = amzaRing;
        this.soyService = soyService;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/chord")
    public Response chord() {
        try {
            List<RingHost> ring = amzaRing.getRing("master");
            Collections.sort(ring);
            List<List<Integer>> matrix = new ArrayList<>();
            for (RingHost r : ring) {
                List<Integer> weights = new ArrayList();
                for (RingHost r2 : ring) {
                    if (host.equals(r)) {
                        if (!host.equals(r2)) {
                            weights.add((int) (amzaStats.getTotalTakes(r2) + amzaStats.getTotalOffered(r2)));
                        } else {
                            weights.add(0);
                        }
                    } else if (host.equals(r2)) {
                        weights.add((int) (amzaStats.getTotalTakes(r) + amzaStats.getTotalOffered(r)));
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
            List<RingHost> ring = amzaRing.getRing("master");
            Collections.sort(ring);

            Map<String, Integer> index = new HashMap<>();
            Map<String, Object> arcData = new HashMap<>();
            int i = 0;
            int g = 1;
            List<Map<String, Object>> nodes = new ArrayList();
            for (RingHost r : ring) {
                String name = r.getHost() + ":" + r.getPort();
                addNode(nodes, index, name + "-received", i, g);
                i++;
                addNode(nodes, index, name + "-take", i, g);
                i++;
                if (r.equals(host)) {
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
            String sourceName = hostAsString(host);
            for (RingHost r : ring) {
                long totalOffered = amzaStats.getTotalOffered(r);
                if (totalOffered > 0) {
                    range.value(totalOffered);
                }

                long totalTakes = amzaStats.getTotalTakes(r);
                if (totalTakes > 0) {
                    range.value(totalOffered);
                }
            }

            List<Map<String, Integer>> links = new ArrayList();
            Integer root = index.get(sourceName);
            for (RingHost r : ring) {
                long totalOffered = amzaStats.getTotalOffered(r);
                long totalTakes = amzaStats.getTotalTakes(r);
                long total = totalOffered + totalTakes; // TODO use applied from replicated vs took
                if (totalOffered > 0) {
                    Integer source = index.get(sourceName + "-replicate");
                    Integer target = index.get(hostAsString(r) + "-received");
                    addLink(source, target, range, totalOffered, links);
                    addLink(root, target, range, total, links);

                }

                if (totalTakes > 0) {
                    Integer source = index.get(sourceName + "-take");
                    Integer target = index.get(hostAsString(r) + "-took");
                    addLink(source, target, range, totalTakes, links);
                    addLink(root, target, range, total, links);
                }

            }
            arcData.put("nodes", nodes);
            arcData.put("links", links);

            return Response.ok(mapper.writeValueAsString(arcData), MediaType.APPLICATION_JSON).build();
        } catch (Exception x) {
            LOG.error("Failed genrating chords.", x);
            return Response.serverError().build();
        }
    }

    private void addLink(Integer source, Integer target, MinMaxLong range, long totalTakes, List<Map<String, Integer>> links) {
        Map<String, Integer> map = new HashMap<>();
        map.put("source", source);
        map.put("target", target);
        map.put("value", (int) (range.zeroToOne(totalTakes) * 10));
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
        return new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
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
