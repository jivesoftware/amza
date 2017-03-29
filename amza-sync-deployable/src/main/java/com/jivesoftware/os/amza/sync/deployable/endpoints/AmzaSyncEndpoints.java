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
import com.jivesoftware.os.amza.sync.api.AmzaSyncStatus;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncPartitionConfigStorage;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncSender;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncSenderMap;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncSenders;
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

    private final AmzaSyncSenderMap configStorage;
    private final AmzaSyncPartitionConfigStorage partitionConfigStorage;
    private final AmzaSyncSenders syncSenders;
    private final BAInterner interner;

    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public AmzaSyncEndpoints(@Context AmzaSyncSenderMap configStorage,
        @Context AmzaSyncPartitionConfigStorage partitionConfigStorage,
        @Context AmzaSyncSenders syncSenders,
        @Context BAInterner interner) {

        this.configStorage = configStorage;
        this.partitionConfigStorage = partitionConfigStorage;
        this.syncSenders = syncSenders;
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
            return Response.ok("{}").build();
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/cursors/{syncspaceName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCursors(@PathParam("syncspaceName") String syncspaceName) {
        try {
            AmzaSyncSender sender = syncSenders.getSender(syncspaceName);
            Map<String, AmzaSyncStatus> map = Maps.newHashMap();
            if (sender != null) {
                sender.streamCursors(null, null, (fromPartitionName, toPartitionName, timestamp, cursor) -> {
                    map.put(AmzaSyncPartitionTuple.toKeyString(new AmzaSyncPartitionTuple(fromPartitionName, toPartitionName)),
                        new AmzaSyncStatus(timestamp, cursor.maxTimestamp, cursor.maxVersion, cursor.taking));
                    return true;
                });
            }
            return Response.ok(map).build();
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/partitionCursors/{syncspaceName}/{fromPartitionNameBase64}/{toPartitionNameBase64}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPartitionCursors(@PathParam("syncspaceName") String syncspaceName,
        @PathParam("fromPartitionNameBase64") String fromPartitionNameBase64,
        @PathParam("toPartitionNameBase64") String toPartitionNameBase64) {
        try {
            PartitionName from = PartitionName.fromBase64(fromPartitionNameBase64, interner);
            PartitionName to = PartitionName.fromBase64(toPartitionNameBase64, interner);

            AmzaSyncSender sender = syncSenders.getSender(syncspaceName);
            Map<String, AmzaSyncStatus> map = Maps.newHashMap();
            if (sender != null) {
                sender.streamCursors(from, to, (fromPartitionName, toPartitionName, timestamp, cursor) -> {
                    map.put(AmzaSyncPartitionTuple.toKeyString(new AmzaSyncPartitionTuple(fromPartitionName, toPartitionName)),
                        new AmzaSyncStatus(timestamp, cursor.maxTimestamp, cursor.maxVersion, cursor.taking));
                    return true;
                });
            }
            return Response.ok(map).build();
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
        @PathParam("toPartitionNameBase64") String toPartitionNameBase64,
        AmzaSyncPartitionConfig config) {
        try {
            PartitionName from = PartitionName.fromBase64(fromPartitionNameBase64, interner);
            PartitionName to = PartitionName.fromBase64(toPartitionNameBase64, interner);

            AmzaSyncSenderConfig amzaSyncSenderConfig = configStorage.get(syncspaceName);
            if (amzaSyncSenderConfig == null) {
                LOG.warn("Rejected add from:{} to:{} for unknown syncspace:{}", from, to, syncspaceName);
                return Response.status(Status.BAD_REQUEST).entity("Syncspace does not exist: " + syncspaceName).build();
            }
            if (from.equals(to) && amzaSyncSenderConfig.loopback) {
                LOG.warn("Rejected self-referential add for:{} for syncspace:{}", from, syncspaceName);
                return Response.status(Status.BAD_REQUEST).entity("Loopback syncspace cannot be self-referential").build();
            }

            partitionConfigStorage.multiPut(syncspaceName, ImmutableMap.of(new AmzaSyncPartitionTuple(from, to), config));
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to add.", e);
            return Response.serverError().build();
        }
    }

    @DELETE
    @Path("/delete/{syncspaceName}/{fromPartitionNameBase64}/{toPartitionNameBase64}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete(@PathParam("syncspaceName") String syncspaceName,
        @PathParam("fromPartitionNameBase64") String fromPartitionNameBase64,
        @PathParam("toPartitionNameBase64") String toPartitionNameBase64) {
        try {
            PartitionName from = PartitionName.fromBase64(fromPartitionNameBase64, interner);
            PartitionName to = PartitionName.fromBase64(toPartitionNameBase64, interner);
            partitionConfigStorage.multiRemove(syncspaceName, ImmutableList.of(new AmzaSyncPartitionTuple(from, to)));
            return responseHelper.jsonResponse("Success");
        } catch (Exception e) {
            LOG.error("Failed to get.", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/reset/{syncspaceName}/{partitionNameBase64}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response postReset(@PathParam("syncspaceName") String syncspaceName,
        @PathParam("partitionNameBase64") String partitionNameBase64) {
        try {
            if (syncSenders != null) {
                PartitionName partitionName = PartitionName.fromBase64(partitionNameBase64, interner);
                AmzaSyncSender sender = syncSenders.getSender(syncspaceName);
                boolean result = sender != null && sender.resetCursors(partitionName);
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
