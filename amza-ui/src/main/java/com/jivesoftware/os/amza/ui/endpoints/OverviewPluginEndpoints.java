package com.jivesoftware.os.amza.ui.endpoints;

import com.jivesoftware.os.amza.ui.region.OverviewRegion;
import com.jivesoftware.os.amza.ui.region.OverviewRegion.OverviewInput;
import com.jivesoftware.os.amza.ui.soy.SoyService;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/amza/ui/overview")
public class OverviewPluginEndpoints {

    private final SoyService soyService;
    private final OverviewRegion pluginRegion;

    public OverviewPluginEndpoints(@Context SoyService soyService, @Context OverviewRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response ring() {
        String rendered = soyService.renderPlugin(pluginRegion,new OverviewInput());
        return Response.ok(rendered).build();
    }

}
