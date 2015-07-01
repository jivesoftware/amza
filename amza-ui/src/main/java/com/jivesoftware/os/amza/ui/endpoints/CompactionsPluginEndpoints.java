package com.jivesoftware.os.amza.ui.endpoints;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.ui.region.CompactionsPluginRegion;
import com.jivesoftware.os.amza.ui.region.CompactionsPluginRegion.CompactionsPluginRegionInput;
import com.jivesoftware.os.amza.ui.soy.SoyService;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public Response filter(@QueryParam("cluster") @DefaultValue("") String cluster,
        @QueryParam("host") @DefaultValue("") String host,
        @QueryParam("service") @DefaultValue("") String service) {
        String rendered = soyService.renderPlugin(pluginRegion,
            Optional.of(new CompactionsPluginRegionInput(cluster, host, service)));
        return Response.ok(rendered).build();
    }


}
