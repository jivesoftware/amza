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

import com.google.common.collect.Iterables;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPI;
import com.jivesoftware.os.amza.shared.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
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
    public Response set(@QueryParam("partition") String partition,
        @QueryParam("key") String key,
        @QueryParam("value") String value) {
        try {
            AmzaPartitionAPI partitionAPI = createPartitionIfAbsent(partition);
            String[] keys = key.split(",");
            String[] values = value.split(",");
            AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
            for (int i = 0; i < keys.length; i++) {
                updates.set(new WALKey(keys[i].getBytes()), values[i].getBytes(), -1);
            }
            partitionAPI.commit(updates, 1, 30000);
            return Response.ok("ok", MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to set partition:" + partition + " key:" + key + " value:" + value, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to set partition:" + partition + " key:" + key + " value:" + value, x);
        }
    }

    @POST
    @Consumes("application/json")
    @Path("/multiSet/{partition}")
    public Response multiSet(@PathParam("partition") String partition, Map<String, String> values) {
        try {
            AmzaPartitionAPI partitionAPI = createPartitionIfAbsent(partition);
            AmzaPartitionUpdates updates = new AmzaPartitionUpdates();

            updates.setAll(Iterables.transform(values.entrySet(), (input) -> new AbstractMap.SimpleEntry<>(new WALKey(input.getKey().getBytes()),
                input.getValue().getBytes())), -1);
            partitionAPI.commit(updates, 1, 30000);

            return Response.ok("ok", MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to set partition:" + partition + " values:" + values, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to set partition:" + partition + " values:" + values, x);
        }
    }

    @GET
    @Consumes("application/json")
    @Path("/get")
    public Response get(@QueryParam("partition") String partition,
        @QueryParam("key") String key) {
        try {
            String[] keys = key.split(",");
            List<WALKey> rawKeys = new ArrayList<>();
            for (String k : keys) {
                rawKeys.add(new WALKey(k.getBytes()));
            }

            AmzaPartitionAPI partitionAPI = createPartitionIfAbsent(partition);
            List<byte[]> got = new ArrayList<>();
            partitionAPI.get(rawKeys, (key1, value, timestamp) -> {
                got.add(value);
                return true;
            });
            return ResponseHelper.INSTANCE.jsonResponse(got);
        } catch (Exception x) {
            LOG.warn("Failed to get partition:" + partition + " key:" + key, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to get partition:" + partition + " key:" + key, x);
        }
    }

    @GET
    @Consumes("application/json")
    @Path("/remove")
    public Response remove(@QueryParam("partition") String partition,
        @QueryParam("key") String key) {
        try {
            AmzaPartitionAPI partitionAPI = createPartitionIfAbsent(partition);
            AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
            updates.remove(new WALKey(key.getBytes()), -1);
            partitionAPI.commit(updates, 1, 30000);
            return Response.ok("removed " + key, MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to remove partition:" + partition + " key:" + key, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to remove partition:" + partition + " key:" + key, x);
        }
    }

    AmzaPartitionAPI createPartitionIfAbsent(String simplePartitionName) throws Exception {

        int ringSize = amzaService.getRingReader().getRingSize("default");
        int systemRingSize = amzaService.getRingReader().getRingSize(AmzaRingReader.SYSTEM_RING);
        if (ringSize < systemRingSize) {
            amzaService.getRingWriter().buildRandomSubRing("default", systemRingSize);
        }

        WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(new PrimaryIndexDescriptor("berkeleydb", 0, false, null),
            null, 1000, 1000);

        PartitionName partitionName = new PartitionName(false, "default", simplePartitionName);
        amzaService.setPropertiesIfAbsent(partitionName, new PartitionProperties(storageDescriptor, 1, false));
        long maxSleep = TimeUnit.SECONDS.toMillis(30); // TODO expose to config
        amzaService.awaitOnline(partitionName, maxSleep);
        return amzaService.getPartition(partitionName);
    }
}
