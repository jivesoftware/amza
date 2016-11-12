package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
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

    public AmzaSyncUIEndpoints(@Context AmzaSyncUIService syncUIService) {
        this.syncUIService = syncUIService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response get() {
        try {
            String rendered = syncUIService.render();
            return Response.ok(rendered).build();
        } catch (Throwable t) {
            LOG.error("Failed to get", t);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/status")
    @Produces(MediaType.TEXT_HTML)
    public Response getStatus() {
        try {
            String rendered = syncUIService.renderStatus(null);
            return Response.ok(rendered).build();
        } catch (Throwable t) {
            LOG.error("Failed to getStatus", t);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/status/{ringName}/{partitionName}")
    @Produces(MediaType.TEXT_HTML)
    public Response getStatus(@PathParam("ringName") String ringName,
        @PathParam("partitionName") String partitionName) {
        try {
            String rendered = syncUIService.renderStatus(new PartitionName(false,
                ringName.getBytes(StandardCharsets.UTF_8),
                partitionName.getBytes(StandardCharsets.UTF_8)));
            return Response.ok(rendered).build();
        } catch (Throwable t) {
            LOG.error("Failed to getStatus({}, {})", new Object[] { ringName, partitionName }, t);
            return Response.serverError().build();
        }
    }

}
