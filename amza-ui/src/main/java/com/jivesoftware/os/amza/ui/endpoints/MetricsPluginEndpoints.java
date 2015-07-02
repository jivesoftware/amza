package com.jivesoftware.os.amza.ui.endpoints;

import com.jivesoftware.os.amza.ui.region.MetricsPluginRegion;
import com.jivesoftware.os.amza.ui.region.MetricsPluginRegion.MetricsPluginRegionInput;
import com.jivesoftware.os.amza.ui.soy.SoyService;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response filter(@QueryParam("partitionName") @DefaultValue("") String partitionName) {
        String rendered = soyService.renderPlugin(pluginRegion,
            new MetricsPluginRegionInput(partitionName));
        return Response.ok(rendered).build();
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/stats/")
    public Response stats(@QueryParam("partitionName") String partitionName) {
        return Response.ok(pluginRegion.renderStats(partitionName)).build();
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/overview")
    public Response overview() throws Exception {
        return Response.ok(pluginRegion.renderOverview()).build();
    }

}
