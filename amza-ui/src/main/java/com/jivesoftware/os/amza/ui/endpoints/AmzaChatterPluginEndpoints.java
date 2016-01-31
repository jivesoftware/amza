package com.jivesoftware.os.amza.ui.endpoints;

import com.jivesoftware.os.amza.ui.region.AmzaChatterPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaChatterPluginRegion.ChatterPluginRegionInput;
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
@Path("/amza/ui/chatter")
public class AmzaChatterPluginEndpoints {

    private final SoyService soyService;
    private final AmzaChatterPluginRegion pluginRegion;

    public AmzaChatterPluginEndpoints(@Context SoyService soyService, @Context AmzaChatterPluginRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response chatter() {
        String rendered = soyService.renderPlugin(pluginRegion,new ChatterPluginRegionInput());
        return Response.ok(rendered).build();
    }
}
