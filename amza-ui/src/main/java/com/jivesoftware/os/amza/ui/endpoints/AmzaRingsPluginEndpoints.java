package com.jivesoftware.os.amza.ui.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.ui.region.AmzaRingsPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaRingsPluginRegion.AmzaRingsPluginRegionInput;
import com.jivesoftware.os.amza.ui.soy.SoyService;
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
@Path("/amza/ui/rings")
public class AmzaRingsPluginEndpoints {

    private final SoyService soyService;
    private final AmzaRingsPluginRegion pluginRegion;

    public AmzaRingsPluginEndpoints(@Context SoyService soyService, @Context AmzaRingsPluginRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response ring() {
        String rendered = soyService.renderPlugin(pluginRegion,
            Optional.of(new AmzaRingsPluginRegionInput("", "", "", "")));
        return Response.ok(rendered).build();
    }

    @POST
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response action(@FormParam("ringName") @DefaultValue("default") String ringName,
        @FormParam("status") @DefaultValue("off") String status,
        @FormParam("member") @DefaultValue("") String member,
        @FormParam("action") @DefaultValue("") String action) {
        String rendered = soyService.renderPlugin(pluginRegion,
            Optional.of(new AmzaRingsPluginRegionInput(ringName, status, member, action)));
        return Response.ok(rendered).build();
    }
}
