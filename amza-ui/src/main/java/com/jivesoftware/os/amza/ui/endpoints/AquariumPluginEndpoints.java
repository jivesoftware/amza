package com.jivesoftware.os.amza.ui.endpoints;

import com.jivesoftware.os.amza.ui.region.AquariumPluginRegion;
import com.jivesoftware.os.amza.ui.region.AquariumPluginRegion.AquariumPluginRegionInput;
import com.jivesoftware.os.amza.ui.soy.SoyService;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
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
@Path("/amza/ui/aquarium")
public class AquariumPluginEndpoints {

    private final SoyService soyService;
    private final AquariumPluginRegion pluginRegion;

    public AquariumPluginEndpoints(@Context SoyService soyService, @Context AquariumPluginRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response ring() {
        String rendered = soyService.renderPlugin(pluginRegion,
            new AquariumPluginRegionInput("", "", "0"));
        return Response.ok(rendered).build();
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response action(@FormParam("ringName") @DefaultValue("") String ringName,
        @FormParam("partitionName") @DefaultValue("") String partitionName,
        @FormParam("partitionVersion") @DefaultValue("0") String hexPartitionVersion) {
        String rendered = soyService.renderPlugin(pluginRegion, new AquariumPluginRegionInput(ringName, partitionName, hexPartitionVersion));
        return Response.ok(rendered).build();
    }
}
