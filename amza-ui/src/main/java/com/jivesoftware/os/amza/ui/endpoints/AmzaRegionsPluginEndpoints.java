package com.jivesoftware.os.amza.ui.endpoints;

import com.jivesoftware.os.amza.ui.region.AmzaPartitionsPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaPartitionsPluginRegion.AmzaPartitionsPluginRegionInput;
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
@Path("/amza/ui/partitions")
public class AmzaRegionsPluginEndpoints {

    private final SoyService soyService;
    private final AmzaPartitionsPluginRegion region;

    public AmzaRegionsPluginEndpoints(@Context SoyService soyService, @Context AmzaPartitionsPluginRegion region) {
        this.soyService = soyService;
        this.region = region;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response ring() {
        String rendered = soyService.renderPlugin(region, new AmzaPartitionsPluginRegionInput("", "", "", takeFromFactor));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response action(@FormParam("action") @DefaultValue("") String action,
        @FormParam("ringName") @DefaultValue("") String ringName,
        @FormParam("partitionName") @DefaultValue("") String partitionName) {
        String rendered = soyService.renderPlugin(region, new AmzaPartitionsPluginRegionInput(action, ringName, partitionName, takeFromFactor));
        return Response.ok(rendered).build();
    }
}
