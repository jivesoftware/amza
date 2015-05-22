package com.jivesoftware.os.amza.ui.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.ui.region.AmzaClusterPluginRegion;
import com.jivesoftware.os.amza.ui.region.AmzaClusterPluginRegion.AmzaClusterPluginRegionInput;
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
@Path("/amza/ui/cluster")
public class AmzaClusterPluginEndpoints {

    private final SoyService soyService;
    private final AmzaClusterPluginRegion pluginRegion;

    public AmzaClusterPluginEndpoints(@Context SoyService soyService, @Context AmzaClusterPluginRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response ring() {
        String rendered = soyService.renderPlugin(pluginRegion,
            Optional.of(new AmzaClusterPluginRegionInput("", "", 0, "")));
        return Response.ok(rendered).build();
    }

    @POST
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response action(@FormParam("member") @DefaultValue("") String member,
        @FormParam("host") @DefaultValue("") String host,
        @FormParam("port") @DefaultValue("0") int port,
        @FormParam("action") @DefaultValue("") String action) {
        String rendered = soyService.renderPlugin(pluginRegion,
            Optional.of(new AmzaClusterPluginRegionInput(member, host, port, action)));
        return Response.ok(rendered).build();
    }
}
