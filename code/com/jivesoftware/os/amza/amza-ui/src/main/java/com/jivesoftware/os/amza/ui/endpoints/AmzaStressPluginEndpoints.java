package com.jivesoftware.os.amza.ui.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.ui.region.AmzaStressPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaStressPluginRegion.AmzaStressPluginRegionInput;
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
@Path("/amza/ui/stress")
public class AmzaStressPluginEndpoints {

    private final SoyService soyService;
    private final AmzaStressPluginRegion pluginRegion;

    public AmzaStressPluginEndpoints(@Context SoyService soyService, @Context AmzaStressPluginRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response ring() {
        String rendered = soyService.renderPlugin(pluginRegion,
            Optional.of(new AmzaStressPluginRegionInput("", "", 0, 0, 0, 0, "")));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response action(@FormParam("name") @DefaultValue("") String name,
        @FormParam("regionPrefix") @DefaultValue("") String regionPrefix,
        @FormParam("numBatches") @DefaultValue("1") int numBatches,
        @FormParam("batchSize") @DefaultValue("1") int batchSize,
        @FormParam("numRegions") @DefaultValue("1") int numRegions,
        @FormParam("numThreadsPerRegion") @DefaultValue("1") int numThreadsPerRegion,
        @FormParam("action") @DefaultValue("") String action) {
        String rendered = soyService.renderPlugin(pluginRegion,
            Optional.of(new AmzaStressPluginRegionInput(name.trim(), regionPrefix.trim(), numBatches, batchSize, numRegions, numThreadsPerRegion, action)));
        return Response.ok(rendered).build();
    }
}
