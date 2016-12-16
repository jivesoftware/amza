package com.jivesoftware.os.amza.ui.endpoints;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.ui.region.MetricsPluginRegion;
import com.jivesoftware.os.amza.ui.region.MetricsPluginRegion.MetricsPluginRegionInput;
import com.jivesoftware.os.amza.ui.soy.SoyService;
import java.nio.charset.StandardCharsets;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 *
 */
@Singleton
@Path("/amza/ui/metrics")
public class MetricsPluginEndpoints {

    private final SoyService soyService;
    private final MetricsPluginRegion pluginRegion;

    public MetricsPluginEndpoints(@Context SoyService soyService, @Context MetricsPluginRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response filter(@QueryParam("partitionName") @DefaultValue("") String partitionName,
        @QueryParam("ringName") @DefaultValue("") String ringName,
        @QueryParam("exact") @DefaultValue("false") boolean exact,
        @QueryParam("visualize") @DefaultValue("false") boolean visualize) {
        String rendered = soyService.renderPlugin(pluginRegion,
            new MetricsPluginRegionInput(ringName, partitionName, exact, visualize));
        return Response.ok(rendered).build();
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/stats")
    public Response stats(@QueryParam("partitionName") String partitionName,
        @QueryParam("exact") boolean exact) {
        return Response.ok(pluginRegion.renderStats(partitionName, exact)).build();
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/overview")
    public Response overview() throws Exception {
        return Response.ok(pluginRegion.renderOverview()).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    @Path("/abandon")
    public Response abandon(@FormParam("ringName") @DefaultValue("") String ringName,
        @FormParam("partitionName") @DefaultValue("") String partitionName) throws Exception {
        byte[] ringNameBytes = ringName.getBytes(StandardCharsets.UTF_8);
        byte[] partitionNameBytes = partitionName.getBytes(StandardCharsets.UTF_8);
        if (ringNameBytes.length > 0 && partitionNameBytes.length > 0) {
            if (pluginRegion.abandonPartition(new PartitionName(false, ringNameBytes, partitionNameBytes))) {
                return Response.ok("Success").build();
            }
        }
        return Response.status(Status.EXPECTATION_FAILED).build();
    }

}
