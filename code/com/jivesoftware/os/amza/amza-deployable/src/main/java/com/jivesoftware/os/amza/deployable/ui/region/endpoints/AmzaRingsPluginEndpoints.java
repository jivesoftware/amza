package com.jivesoftware.os.amza.deployable.ui.region.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.deployable.ui.region.AmzaRingsPluginRegion;
import com.jivesoftware.os.amza.deployable.ui.region.AmzaRingsPluginRegion.AmzaRingsPluginRegionInput;
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
@Path("/ui/rings")
public class AmzaRingsPluginEndpoints {

    private final SoyService soyService;
    private final AmzaRingsPluginRegion pluginRegion;

    public AmzaRingsPluginEndpoints(@Context SoyService soyService, @Context AmzaRingsPluginRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response ring() {
        String rendered = soyService.renderPlugin(pluginRegion,
            Optional.of(new AmzaRingsPluginRegionInput("", "", "", "", "")));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response action(@FormParam("ringName") @DefaultValue("default") String ringName,
        @FormParam("status") @DefaultValue("off") String status,
        @FormParam("host") @DefaultValue("") String host,
        @FormParam("port") @DefaultValue("") String port,
        @FormParam("action") @DefaultValue("") String action) {
        String rendered = soyService.renderPlugin(pluginRegion,
            Optional.of(new AmzaRingsPluginRegionInput(ringName, status, host, port, action)));
        return Response.ok(rendered).build();
    }
}
