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

import com.jivesoftware.os.amza.service.AmzaRegion;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
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
            List<Entry<WALKey, byte[]>> entries = new ArrayList<>();
            String[] keys = key.split(",");
            String[] values = value.split(",");
            for (int i = 0; i < keys.length; i++) {
                entries.add(new AbstractMap.SimpleEntry<>(new WALKey(keys[i].getBytes()), values[i].getBytes()));
            }
            amzaRegion.set(entries);
            return Response.ok("ok", MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to set region:" + region + " key:" + key + " value:" + value, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to set region:" + region + " key:" + key + " value:" + value, x);
        }
    }

    @GET
    @Consumes("application/json")
    @Path("/get")
    public Response get(@QueryParam("region") String region,
        @QueryParam("key") String key) {
        try {
            String[] keys = key.split(",");
            List<WALKey> rowKeys = new ArrayList<>();
            for (String k : keys) {
                rowKeys.add(new WALKey(k.getBytes()));
            }

            AmzaRegion amzaRegion = createRegionIfAbsent(region);
            List<byte[]> got = amzaRegion.get(rowKeys);
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
            return Response.ok(amzaRegion.remove(new WALKey(key.getBytes())), MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to remove region:" + region + " key:" + key, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to remove region:" + region + " key:" + key, x);
        }
    }

    AmzaRegion createRegionIfAbsent(String regionName) throws Exception {

        List<RingHost> ring = amzaService.getAmzaRing().getRing("default");
        if (ring.isEmpty()) {
            amzaService.getAmzaRing().buildRandomSubRing("default", amzaService.getAmzaRing().getRing("system").size());
        }

        WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(new PrimaryIndexDescriptor("berkeleydb", 0, false, null),
            null, 1000, 1000);

        return amzaService.createRegionIfAbsent(new RegionName(false, "default", regionName),
            new RegionProperties(storageDescriptor, 1, 1, false)
        );
    }
}
