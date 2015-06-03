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
import com.jivesoftware.os.amza.service.AmzaRegion;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.AmzaRegionUpdates;
import com.jivesoftware.os.amza.shared.region.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.region.RegionName;
import com.jivesoftware.os.amza.shared.region.RegionProperties;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
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
    public Response set(@QueryParam("region") String region,
        @QueryParam("key") String key,
        @QueryParam("value") String value) {
        try {
            AmzaRegion amzaRegion = createRegionIfAbsent(region);
            String[] keys = key.split(",");
            String[] values = value.split(",");
            AmzaRegionUpdates updates = new AmzaRegionUpdates();
            for (int i = 0; i < keys.length; i++) {
                updates.set(keys[i].getBytes(), values[i].getBytes(), -1);
            }
            amzaRegion.commit(updates);
            return Response.ok("ok", MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to set region:" + region + " key:" + key + " value:" + value, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to set region:" + region + " key:" + key + " value:" + value, x);
        }
    }

    @POST
    @Consumes("application/json")
    @Path("/multiSet/{region}")
    public Response multiSet(@PathParam("region") String region, Map<String, String> values) {
        try {
            AmzaRegion amzaRegion = createRegionIfAbsent(region);
            AmzaRegionUpdates updates = new AmzaRegionUpdates();

            updates.setAll(Iterables.transform(values.entrySet(), (input) -> new AbstractMap.SimpleEntry<>(input.getKey()
                .getBytes(), input.getValue().getBytes())), -1);
            amzaRegion.commit(updates);

            return Response.ok("ok", MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to set region:" + region + " values:" + values, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to set region:" + region + " values:" + values, x);
        }
    }

    @GET
    @Consumes("application/json")
    @Path("/get")
    public Response get(@QueryParam("region") String region,
        @QueryParam("key") String key) {
        try {
            String[] keys = key.split(",");
            List<byte[]> rawKeys = new ArrayList<>();
            for (String k : keys) {
                rawKeys.add(k.getBytes());
            }

            AmzaRegion amzaRegion = createRegionIfAbsent(region);
            List<byte[]> got = new ArrayList<>();
            amzaRegion.get(rawKeys, (rowTxId, key1, scanned) -> {
                got.add(scanned.getValue());
                return true;
            });
            return ResponseHelper.INSTANCE.jsonResponse(got);
        } catch (Exception x) {
            LOG.warn("Failed to get region:" + region + " key:" + key, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to get region:" + region + " key:" + key, x);
        }
    }

    @GET
    @Consumes("application/json")
    @Path("/remove")
    public Response remove(@QueryParam("region") String region,
        @QueryParam("key") String key) {
        try {
            AmzaRegion amzaRegion = createRegionIfAbsent(region);
            AmzaRegionUpdates updates = new AmzaRegionUpdates();
            updates.remove(key.getBytes(), -1);
            amzaRegion.commit(updates);
            return Response.ok("removed " + key, MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to remove region:" + region + " key:" + key, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to remove region:" + region + " key:" + key, x);
        }
    }

    AmzaRegion createRegionIfAbsent(String simpleRegionName) throws Exception {

        int ringSize = amzaService.getAmzaHostRing().getRingSize("default");
        int systemRingSize = amzaService.getAmzaHostRing().getRingSize("system");
        if (ringSize < systemRingSize) {
            amzaService.getAmzaHostRing().buildRandomSubRing("default", systemRingSize);
        }

        WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(new PrimaryIndexDescriptor("berkeleydb", 0, false, null),
            null, 1000, 1000);

        RegionName regionName = new RegionName(false, "default", simpleRegionName);
        amzaService.setPropertiesIfAbsent(regionName, new RegionProperties(storageDescriptor, 1, 1, false));

        AmzaService.AmzaRegionRoute regionRoute = amzaService.getRegionRoute(regionName);
        long start = System.currentTimeMillis();
        long maxSleep = TimeUnit.SECONDS.toMillis(30); // TODO expose to config
        while (regionRoute.orderedRegionHosts.isEmpty() && (System.currentTimeMillis() - start) > maxSleep) {
            Thread.sleep(1000); // Sorry calling thread.
            regionRoute = amzaService.getRegionRoute(regionName);
        }
        if (regionRoute.orderedRegionHosts.isEmpty()) {
            throw new RuntimeException("Region failed to come ONLINE in " + maxSleep);
        }
        return amzaService.getRegion(regionName);
    }
}
