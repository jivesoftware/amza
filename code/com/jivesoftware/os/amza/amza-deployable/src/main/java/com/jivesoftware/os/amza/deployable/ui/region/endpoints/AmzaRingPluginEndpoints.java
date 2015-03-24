package com.jivesoftware.os.amza.deployable.ui.region.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.deployable.ui.region.AmzaRingPluginRegion;
import com.jivesoftware.os.amza.deployable.ui.region.AmzaRingPluginRegion.AmzaRingPluginRegionInput;
import com.jivesoftware.os.amza.deployable.ui.soy.SoyService;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@Singleton
@Path("/ui/ring")
public class AmzaRingPluginEndpoints {

    private final SoyService soyService;
    private final AmzaRingPluginRegion pluginRegion;

    public AmzaRingPluginEndpoints(@Context SoyService soyService, @Context AmzaRingPluginRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response ring() {
        String rendered = soyService.renderPlugin(pluginRegion,
            Optional.of(new AmzaRingPluginRegionInput("", "", "")));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response action(@FormParam("host") @DefaultValue("") String host,
        @FormParam("port") @DefaultValue("") String port,
        @FormParam("action") @DefaultValue("") String action) {
        String rendered = soyService.renderPlugin(pluginRegion,
            Optional.of(new AmzaRingPluginRegionInput(host, port, action)));
        return Response.ok(rendered).build();
    }
}
