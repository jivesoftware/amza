package com.jivesoftware.os.amza.deployable.ui.region.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.deployable.ui.region.HealthPluginRegion;
import com.jivesoftware.os.amza.deployable.ui.region.HealthPluginRegion.HealthPluginRegionInput;
import com.jivesoftware.os.amza.deployable.ui.soy.SoyService;
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
@Path("/ui/metrics")
public class HealthPluginEndpoints {

    private final SoyService soyService;
    private final HealthPluginRegion pluginRegion;

    public HealthPluginEndpoints(@Context SoyService soyService, @Context HealthPluginRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response filter(@QueryParam("cluster") @DefaultValue("") String cluster,
        @QueryParam("host") @DefaultValue("") String host,
        @QueryParam("service") @DefaultValue("") String service) {
        String rendered = soyService.renderPlugin(pluginRegion,
            Optional.of(new HealthPluginRegionInput(cluster, host, service)));
        return Response.ok(rendered).build();
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/stats")
    public Response stats() {
        return Response.ok(pluginRegion.renderStats()).build();
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/overview")
    public Response overview() throws Exception {
        return Response.ok(pluginRegion.renderOverview()).build();
    }

}
