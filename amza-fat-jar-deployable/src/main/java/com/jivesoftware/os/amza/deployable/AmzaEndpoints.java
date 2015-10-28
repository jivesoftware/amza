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

import com.google.common.base.Splitter;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.berkeleydb.BerkeleyDBWALIndexProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.shared.Partition;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path("/amza")
public class AmzaEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaService amzaService;

    public AmzaEndpoints(@Context AmzaService amzaService) {
        this.amzaService = amzaService;
    }

    @GET
    @Consumes("application/json")
    @Path("/set")
    public Response set(@QueryParam("ring") @DefaultValue("default") String ring,
        @QueryParam("indexClassName") @DefaultValue(BerkeleyDBWALIndexProvider.INDEX_CLASS_NAME) String indexClassName,
        @QueryParam("partition") String partitionName,
        @QueryParam("consistency") @DefaultValue("none") String consistency,
        @QueryParam("requireConsistency") @DefaultValue("true") boolean requireConsistency,
        @QueryParam("key") String key,
        @QueryParam("value") String value) {
        try {
            Partition partition = createPartitionIfAbsent(ring, indexClassName, partitionName, Consistency.valueOf(consistency), requireConsistency);
            String[] keys = key.split(",");
            String[] values = value.split(",");
            AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
            for (int i = 0; i < keys.length; i++) {
                //TODO prefix
                updates.set(keys[i].getBytes(StandardCharsets.UTF_8), values[i].getBytes(StandardCharsets.UTF_8), -1);
            }
            partition.commit(Consistency.valueOf(consistency), null, updates, 30000);
            return Response.ok("ok", MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to set partition:" + partitionName + " key:" + key + " value:" + value, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to set partition:" + partitionName + " key:" + key + " value:" + value, x);
        }
    }

    @POST
    @Consumes("application/json")
    @Path("/multiSet/{partition}")
    public Response multiSet(
        @QueryParam("indexClassName") @DefaultValue(BerkeleyDBWALIndexProvider.INDEX_CLASS_NAME) String indexClassName,
        @PathParam("partition") String partitionName,
        @QueryParam("consistency") @DefaultValue("none") String consistency,
        @QueryParam("requireConsistency") @DefaultValue("true") boolean requireConsistency,
        Map<String, String> values) {
        try {
            Partition partition = createPartitionIfAbsent("default", indexClassName, partitionName, Consistency.valueOf(consistency), requireConsistency);
            AmzaPartitionUpdates updates = new AmzaPartitionUpdates();

            for (Map.Entry<String, String> entry : values.entrySet()) {
                //TODO prefix
                updates.set(entry.getKey().getBytes(StandardCharsets.UTF_8), entry.getValue().getBytes(StandardCharsets.UTF_8), -1);
            }
            partition.commit(Consistency.valueOf(consistency), null, updates, 30000);

            return Response.ok("ok", MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to set partition:" + partitionName + " values:" + values, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to set partition:" + partitionName + " values:" + values, x);
        }
    }

    @POST
    @Consumes("application/json")
    @Path("/multiSet/{ring}/{partition}")
    public Response multiSet(
        @QueryParam("indexClassName") @DefaultValue(BerkeleyDBWALIndexProvider.INDEX_CLASS_NAME) String indexClassName,
        @PathParam("partition") String partitionName,
        @QueryParam("consistency") @DefaultValue("none") String consistency,
        @QueryParam("requireConsistency") @DefaultValue("true") boolean requireConsistency,
        @PathParam("ring") String ring,
        Map<String, String> values) {
        try {
            Partition partition = createPartitionIfAbsent(ring, indexClassName, partitionName, Consistency.valueOf(consistency), requireConsistency);
            AmzaPartitionUpdates updates = new AmzaPartitionUpdates();

            for (Map.Entry<String, String> entry : values.entrySet()) {
                //TODO prefix
                updates.set(entry.getKey().getBytes(StandardCharsets.UTF_8), entry.getValue().getBytes(StandardCharsets.UTF_8), -1);
            }
            partition.commit(Consistency.valueOf(consistency), null, updates, 30000);

            return Response.ok("ok", MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to set partition:" + partitionName + " values:" + values, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to set partition:" + partitionName + " values:" + values, x);
        }
    }

    @GET
    @Consumes("application/json")
    @Path("/get")
    public Response get(@QueryParam("ring") @DefaultValue("default") String ring,
        @QueryParam("indexClassName") @DefaultValue(BerkeleyDBWALIndexProvider.INDEX_CLASS_NAME) String indexClassName,
        @QueryParam("partition") String partitionName,
        @QueryParam("consistency") @DefaultValue("none") String consistency,
        @QueryParam("requireConsistency") @DefaultValue("true") boolean requireConsistency,
        @QueryParam("key") String key) {
        try {
            Partition partition = createPartitionIfAbsent(ring, indexClassName, partitionName, Consistency.valueOf(consistency), requireConsistency);
            List<String> got = new ArrayList<>();
            //TODO prefix
            partition.get(Consistency.valueOf(consistency), null,
                stream -> {
                    for (String s : Splitter.on(',').split(key)) {
                        if (!stream.stream(s.getBytes(StandardCharsets.UTF_8))) {
                            return false;
                        }
                    }
                    return true;
                },
                (_prefix, _key, value, timestamp, tombstoned, version) -> {
                    if (timestamp != -1 && !tombstoned) {
                        got.add(new String(value, StandardCharsets.UTF_8));
                    }
                    return true;
                });
            return ResponseHelper.INSTANCE.jsonResponse(got);
        } catch (Exception x) {
            LOG.warn("Failed to get partition:" + partitionName + " key:" + key, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to get partition:" + partitionName + " key:" + key, x);
        }
    }

    @GET
    @Consumes("application/json")
    @Path("/remove")
    public Response remove(@QueryParam("ring") @DefaultValue("default") String ring,
        @QueryParam("indexClassName") @DefaultValue(BerkeleyDBWALIndexProvider.INDEX_CLASS_NAME) String indexClassName,
        @QueryParam("partition") String partitionName,
        @QueryParam("consistency") @DefaultValue("none") String consistency,
        @QueryParam("requireConsistency") @DefaultValue("true") boolean requireConsistency,
        @QueryParam("key") String key) {
        try {
            Partition partition = createPartitionIfAbsent(ring, indexClassName, partitionName, Consistency.valueOf(consistency), requireConsistency);
            AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
            //TODO prefix
            updates.remove(key.getBytes(StandardCharsets.UTF_8), -1);
            partition.commit(Consistency.valueOf(consistency), null, updates, 30000);
            return Response.ok("removed " + key, MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to remove partition:" + partitionName + " key:" + key, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to remove partition:" + partitionName + " key:" + key, x);
        }
    }

    Partition createPartitionIfAbsent(String ringName, String indexClassName,
        String simplePartitionName, Consistency consistency, boolean requireConsistency) throws Exception {

        int ringSize = amzaService.getRingReader().getRingSize(ringName.getBytes(StandardCharsets.UTF_8));
        int systemRingSize = amzaService.getRingReader().getRingSize(AmzaRingReader.SYSTEM_RING);
        if (ringSize < systemRingSize) {
            amzaService.getRingWriter().buildRandomSubRing(ringName.getBytes(StandardCharsets.UTF_8), systemRingSize);
        }

        WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(false,
            new PrimaryIndexDescriptor(indexClassName, 0, false, null),
            null, 1000, 1000);

        PartitionName partitionName = new PartitionName(false, ringName.getBytes(StandardCharsets.UTF_8), simplePartitionName.getBytes(StandardCharsets.UTF_8));
        amzaService.setPropertiesIfAbsent(partitionName, new PartitionProperties(storageDescriptor, consistency, requireConsistency, 1, false));
        long maxSleep = TimeUnit.SECONDS.toMillis(30); // TODO expose to config
        amzaService.awaitOnline(partitionName, maxSleep);
        return amzaService.getPartition(partitionName);
    }
}
