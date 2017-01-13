/*
 * Copyright 2014 Jive Software Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.sync.deployable.endpoints;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.sync.api.AmzaSyncPartitionConfig;
import com.jivesoftware.os.amza.sync.api.AmzaSyncPartitionTuple;
import com.jivesoftware.os.amza.sync.api.AmzaSyncSenderConfig;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncPartitionConfigStorage;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncSender;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncSenderConfigStorage;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.util.Map;
import java.util.Map.Entry;
import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * @author jonathan
 */
@Singleton
@Path("/amza/sync")
public class AmzaSyncEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaSyncSenderConfigStorage configStorage;
    private final AmzaSyncPartitionConfigStorage partitionConfigStorage;
    private final AmzaSyncSender syncSender;
    private final BAInterner interner;

    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public AmzaSyncEndpoints(@Context AmzaSyncSenderConfigStorage configStorage,
        @Context AmzaSyncPartitionConfigStorage partitionConfigStorage,
        @Context AmzaSyncSender syncSender,
        @Context BAInterner interner) {

        this.configStorage = configStorage;
        this.partitionConfigStorage = partitionConfigStorage;
        this.syncSender = syncSender;
        this.interner = interner;
    }

    @GET
    @Path("/syncspace/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listNamesSpaces() {
        try {
            Map<String, AmzaSyncSenderConfig> all = configStorage.getAll();
            return Response.ok(all).build();
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }


    @POST
    @Path("/syncspace/add/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response addsyncspace(@PathParam("name") String name,
        AmzaSyncSenderConfig syncspaceConfig) {
        try {
            configStorage.multiPut(ImmutableMap.of(name, syncspaceConfig));
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }

    @DELETE
    @Path("/syncspace/delete/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deletesyncspace(@PathParam("name") String name) {
        try {
            configStorage.multiRemove(ImmutableList.of(name));
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }


    @GET
    @Path("/list/{syncspaceName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSyncing(@PathParam("syncspaceName") String syncspaceName) {
        try {
            Map<AmzaSyncPartitionTuple, AmzaSyncPartitionConfig> all = partitionConfigStorage.getAll(syncspaceName);
            if (all != null && !all.isEmpty()) {
                Map<String, AmzaSyncPartitionConfig> map = Maps.newHashMap();
                for (Entry<AmzaSyncPartitionTuple, AmzaSyncPartitionConfig> a : all.entrySet()) {
                    map.put(AmzaSyncPartitionTuple.toKeyString(a.getKey()), a.getValue());
                }
                return Response.ok(map).build();
            }
            return Response.noContent().entity("{}").build();
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/add/{syncspaceName}/{fromPartitionNameBase64}/{toPartitionNameBase64}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response post(@PathParam("syncspaceName") String syncspaceName,
        @PathParam("fromPartitionNameBase64") String fromPartitionNameBase64,
        @PathParam("toPartitionNameBase64") String toPartitionNameBase64) {
        try {
            PartitionName from = PartitionName.fromBase64(fromPartitionNameBase64, interner);
            PartitionName to = PartitionName.fromBase64(fromPartitionNameBase64, interner);
            partitionConfigStorage.multiPut(syncspaceName, ImmutableMap.of(new AmzaSyncPartitionTuple(from, to), new AmzaSyncPartitionConfig()));
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to add.", e);
            return Response.serverError().build();
        }
    }

    @DELETE
    @Path("/delete/{syncspaceName}/{fromPartitionNameBase64}/{toPartitionNameBase64}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete(@PathParam("syncspace") String syncspaceName,
        @PathParam("fromPartitionNameBase64") String fromPartitionNameBase64,
        @PathParam("toPartitionNameBase64") String toPartitionNameBase64) {
        try {
            PartitionName from = PartitionName.fromBase64(fromPartitionNameBase64, interner);
            PartitionName to = PartitionName.fromBase64(fromPartitionNameBase64, interner);
            partitionConfigStorage.multiRemove(syncspaceName, ImmutableList.of(new AmzaSyncPartitionTuple(from, to)));
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }


    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get() {
        try {
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/reset/{partitionNameBase64}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response postReset(@PathParam("partitionNameBase64") String partitionNameBase64) {
        try {
            if (syncSender != null) {
                PartitionName partitionName = PartitionName.fromBase64(partitionNameBase64, interner);
                boolean result = syncSender.resetCursors(partitionName);
                return Response.ok(result).build();
            } else {
                return Response.status(Status.SERVICE_UNAVAILABLE).entity("Sender is not enabled").build();
            }
        } catch (Exception e) {
            LOG.error("Failed to reset.", e);
            return Response.serverError().build();
        }
    }

}
