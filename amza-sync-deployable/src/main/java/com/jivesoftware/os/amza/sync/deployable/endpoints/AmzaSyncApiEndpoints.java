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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncReceiver;
import com.jivesoftware.os.amza.sync.deployable.Rows;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;

import java.io.InputStream;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.xerial.snappy.SnappyInputStream;

/**
 * @author jonathan
 */
@Singleton
@Path("/api/sync/v1")
public class AmzaSyncApiEndpoints {
    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaSyncReceiver syncReceiver;
    private final ObjectMapper mapper;
    private final AmzaInterner amzaInterner;

    private final ResponseHelper responseHelper = ResponseHelper.INSTANCE;

    public AmzaSyncApiEndpoints(@Context AmzaSyncReceiver syncReceiver,
        @Context ObjectMapper mapper,
        @Context AmzaInterner amzaInterner) {
        this.syncReceiver = syncReceiver;
        this.mapper = mapper;
        this.amzaInterner = amzaInterner;
    }

    @POST
    @Path("/commit/rows/{partitionNameBase64}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public Response commitRows(@PathParam("partitionNameBase64") String partitionNameBase64,
        InputStream inputStream) throws Exception {
        Rows rows;
        try {
            rows = mapper.readValue(new SnappyInputStream(inputStream), Rows.class);
        } catch (Exception x) {
            LOG.error("Failed decompressing commitRows({})",
                new Object[]{partitionNameBase64}, x);
            return responseHelper.errorResponse("Server error", x);
        }
        try {
            PartitionName partitionName = amzaInterner.internPartitionNameBase64(partitionNameBase64);
            syncReceiver.commitRows(partitionName, rows);
            return responseHelper.jsonResponse("ok");
        } catch (Exception x) {
            LOG.error("Failed calling commitRows({},count:{})",
                new Object[]{partitionNameBase64, rows != null ? rows.size() : null}, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }

    @POST
    @Path("/ensure/partition/{partitionNameBase64}/{ringSize}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response ensurePartition(@PathParam("partitionNameBase64") String partitionNameBase64,
        @PathParam("ringSize") int ringSize,
        PartitionProperties properties) throws Exception {
        try {
            PartitionName partitionName = amzaInterner.internPartitionNameBase64(partitionNameBase64);
            syncReceiver.ensurePartition(partitionName, properties, ringSize);
            return responseHelper.jsonResponse("ok");
        } catch (Exception x) {
            LOG.error("Failed calling ensurePartition({},{})",
                new Object[]{partitionNameBase64, ringSize}, x);
            return responseHelper.errorResponse("Server error", x);
        }
    }
}
