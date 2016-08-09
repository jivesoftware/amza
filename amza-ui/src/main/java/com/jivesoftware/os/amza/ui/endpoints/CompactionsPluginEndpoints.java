package com.jivesoftware.os.amza.ui.endpoints;

import com.jivesoftware.os.amza.ui.region.CompactionsPluginRegion;
import com.jivesoftware.os.amza.ui.region.CompactionsPluginRegion.CompactionsPluginRegionInput;
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
@Path("/amza/ui/compactions")
public class CompactionsPluginEndpoints {

    private final SoyService soyService;
    private final CompactionsPluginRegion pluginRegion;

    public CompactionsPluginEndpoints(@Context SoyService soyService, @Context CompactionsPluginRegion pluginRegion) {
        this.soyService = soyService;
        this.pluginRegion = pluginRegion;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response compactions() {
        String rendered = soyService.renderPlugin(pluginRegion, new CompactionsPluginRegionInput("", "", ""));
        return Response.ok(rendered).build();
    }

    @POST
    @Produces(MediaType.TEXT_HTML)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response filter(@FormParam("action") @DefaultValue("") String action,
        @FormParam("fromIndexClass") @DefaultValue("") String fromIndexClass,
        @FormParam("toIndexClass") @DefaultValue("") String toIndexClass) {
        String rendered = soyService.renderPlugin(pluginRegion, new CompactionsPluginRegionInput(action, fromIndexClass, toIndexClass));
        return Response.ok(rendered).build();
    }

}
