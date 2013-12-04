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
package com.jivesoftware.os.amza.example.deployable.endpoints;

import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaTable;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/example")
public class AmzaExampleEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaService amzaService;

    public AmzaExampleEndpoints(@Context AmzaService amzaService) {
        this.amzaService = amzaService;
    }

    @GET
    @Consumes("application/json")
    @Path("/set")
    public Response set(@QueryParam("table") String table,
            @QueryParam("key") String key,
            @QueryParam("value") String value) {
        try {
            AmzaTable amzaTable = amzaService.getTable(new TableName("master", table, null, null));
            return Response.ok(new String(amzaTable.set(new TableIndexKey(key.getBytes()), value.getBytes()).getKey()), MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to set table:" + table + " key:" + key + " value:" + value, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to set table:" + table + " key:" + key + " value:" + value, x);
        }
    }

    @GET
    @Consumes("application/json")
    @Path("/get")
    public Response get(@QueryParam("table") String table,
            @QueryParam("key") String key) {
        try {
            AmzaTable amzaTable = amzaService.getTable(new TableName("master", table, null, null));
            return Response.ok(new String(amzaTable.get(new TableIndexKey(key.getBytes()))), MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to get table:" + table + " key:" + key, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to get table:" + table + " key:" + key, x);
        }
    }

    @GET
    @Consumes("application/json")
    @Path("/remove")
    public Response remove(@QueryParam("table") String table,
            @QueryParam("key") String key) {
        try {
            AmzaTable amzaTable = amzaService.getTable(new TableName("master", table, null, null));
            return Response.ok(amzaTable.remove(new TableIndexKey(key.getBytes())), MediaType.TEXT_PLAIN).build();
        } catch (Exception x) {
            LOG.warn("Failed to remove table:" + table + " key:" + key, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to remove table:" + table + " key:" + key, x);
        }
    }
}
