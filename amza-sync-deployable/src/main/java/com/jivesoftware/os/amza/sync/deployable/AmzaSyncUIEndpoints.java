package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/ui")
public class AmzaSyncUIEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaSyncUIService syncUIService;
    private final BAInterner interner;

    public AmzaSyncUIEndpoints(@Context AmzaSyncUIService syncUIService, @Context BAInterner interner) {
        this.syncUIService = syncUIService;
        this.interner = interner;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get() {
        String rendered = syncUIService.render();
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/status")
    @Produces(MediaType.TEXT_HTML)
    public Response getStatus() {
        String rendered = syncUIService.renderStatus(null);
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/status/{partitionNameBase64}")
    @Produces(MediaType.TEXT_HTML)
    public Response getStatus(@PathParam("partitionNameBase64") String partitionNameBase64) {
        String rendered = syncUIService.renderStatus(PartitionName.fromBase64(partitionNameBase64, interner));
        return Response.ok(rendered).build();
    }

}
