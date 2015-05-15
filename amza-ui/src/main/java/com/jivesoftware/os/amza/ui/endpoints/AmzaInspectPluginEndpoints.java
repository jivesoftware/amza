package com.jivesoftware.os.amza.ui.endpoints;

import com.jivesoftware.os.amza.ui.region.AmzaInspectPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaInspectPluginRegion.AmzaInspectPluginRegionInput;
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
@Path("/amza/ui/inspect")
public class AmzaInspectPluginEndpoints {

    private final SoyService soyService;
    private final AmzaInspectPluginRegion pluginRegion;

    public AmzaInspectPluginEndpoints(@Context SoyService soyService, @Context AmzaInspectPluginRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response ring() {
        String rendered = soyService.renderPlugin(pluginRegion,
            new AmzaInspectPluginRegionInput(false, "", "", "", 0, 100, ""));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response action(@FormParam("systemRegion") @DefaultValue("false") boolean systemRegion,
        @FormParam("ringName") @DefaultValue("") String ringName,
        @FormParam("regionName") @DefaultValue("") String regionName,
        @FormParam("key") @DefaultValue("") String key,
        @FormParam("offset") @DefaultValue("0") int offset,
        @FormParam("batchSize") @DefaultValue("100") int batchSize,
        @FormParam("action") @DefaultValue("") String action) {
        String rendered = soyService.renderPlugin(pluginRegion,
            new AmzaInspectPluginRegionInput(systemRegion, ringName, regionName, key, offset, batchSize, action));
        return Response.ok(rendered).build();
    }
}
